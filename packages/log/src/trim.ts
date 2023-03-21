import { Cache } from "@dao-xyz/cache";
import PQueue from "p-queue";
import Yallist from "yallist";
import { Entry } from "./entry.js";
import { EntryNode, Values } from "./values.js";

const trimOptionsEqual = (a: TrimOptions, b: TrimOptions) => {
	if (a.type === b.type) {
		if (a.type === "length" && b.type === "length") {
			return (
				a.from === b.from &&
				a.to === b.to &&
				a.filter?.canTrim === b.filter?.canTrim
			);
		}
		if (a.type === "bytelength" && b.type === "bytelength") {
			return (
				a.from === b.from &&
				a.to === b.to &&
				a.filter?.canTrim === b.filter?.canTrim
			);
		}

		if (a.type === "time" && b.type === "time") {
			return a.maxAge === b.maxAge && a.filter?.canTrim === b.filter?.canTrim;
		}
	}
	return false;
};

const trimOptionsStricter = (from: TrimOptions, to: TrimOptions) => {
	if (from.type !== to.type || from.filter?.canTrim !== to.filter?.canTrim) {
		// TODO also check ttl?
		return true; // we don't really know
	}

	if (
		(from.type === "bytelength" || from.type === "length") &&
		(to.type === "bytelength" || to.type === "length")
	) {
		if (from.to > to.to) {
			return true;
		}
		const fromFrom = from.from || from.to;
		const fromTo = to.from || to.to;
		return fromFrom > fromTo;
	} else {
		if (from.type === "time" && to.type === "time") {
			return from.maxAge > to.maxAge;
		}
	}

	throw new Error("Unexpected");
};

export type TrimToLengthOption = { type: "length"; to: number; from?: number };
export type TrimToByteLengthOption = {
	type: "bytelength";
	to: number;
	from?: number;
};

export type TrimToTime = {
	type: "time";
	maxAge: number; // ms
};

export type TrimCondition =
	| TrimToByteLengthOption
	| TrimToLengthOption
	| TrimToTime;
export type TrimCanAppendOption = {
	filter?: {
		canTrim: (gid: string) => Promise<boolean> | boolean;
		cacheId?: () => string;
	};
};
export type TrimOptions = TrimCanAppendOption & TrimCondition;

interface Log<T> {
	values: () => Values<T>;
	deleteNode: (node: EntryNode) => Promise<Entry<T> | undefined>;
}
export class Trim<T> {
	private _trim?: TrimOptions;
	private _canTrimCacheLastNode: EntryNode | undefined | null;
	private _trimLastHead: EntryNode | undefined | null;
	private _trimLastTail: EntryNode | undefined | null;
	private _trimLastOptions: TrimOptions;
	private _trimLastSeed: string | undefined;
	private _canTrimCacheHashBreakpoint: Cache<boolean>;
	private _log: Log<T>;
	private _queue: PQueue;
	constructor(log: Log<T>, options?: TrimOptions) {
		this._log = log;
		this._trim = options;
		this._canTrimCacheHashBreakpoint = new Cache({ max: 1e5 });
		this._queue = new PQueue({ concurrency: 1 });
	}

	deleteFromCache(entry: Entry<T>) {
		if (this._canTrimCacheLastNode?.value.hash === entry.hash) {
			this._canTrimCacheLastNode = this._canTrimCacheLastNode.prev;
		}
	}

	get options() {
		return this._trim;
	}

	private async trimTask(
		option: TrimOptions | undefined = this._trim
	): Promise<Entry<T>[]> {
		if (!option) {
			return [];
		}

		///  TODO Make this method less ugly

		const deleted: Entry<T>[] = [];

		let done: () => Promise<boolean> | boolean;
		const values = this._log.values();
		if (option.type === "length") {
			const to = option.to;
			const from = option.from ?? to;
			if (values.length < from) {
				return [];
			}
			done = () => values.length <= to;
		} else if (option.type == "bytelength") {
			// prune to max sum payload sizes in bytes
			const byteLengthFrom = option.from ?? option.to;

			if (values.byteLength < byteLengthFrom) {
				return [];
			}
			done = () => values.byteLength <= option.to;
		} else if (option.type == "time") {
			const s0 = BigInt(+new Date() * 1e6);
			const maxAge = option.maxAge * 1e6;
			done = async () => {
				if (!values.tail) {
					return true;
				}

				const nodeValue = await values.getEntry(values.tail);

				if (!nodeValue) {
					return true;
				}

				return s0 - nodeValue.metadata.clock.timestamp.wallTime < maxAge;
			};
		} else {
			return [];
		}

		const tail = values.tail;

		if (
			this._trimLastOptions &&
			trimOptionsStricter(this._trimLastOptions, option)
		) {
			this._canTrimCacheHashBreakpoint.clear();
		}

		const seed = option.filter?.cacheId?.();
		const cacheProgress = seed != null;

		let changed = false;
		if (seed !== this._trimLastSeed || !cacheProgress) {
			// Reset caches
			this._canTrimCacheHashBreakpoint.clear();
			this._canTrimCacheLastNode = undefined;
			changed = true;
		} else {
			const trimOptionsChanged =
				!this._trimLastOptions ||
				!trimOptionsEqual(this._trimLastOptions, option);

			const changed =
				this._trimLastHead !== values.head ||
				this._trimLastTail !== values.tail ||
				trimOptionsChanged;
			if (!changed) {
				return [];
			}
		}

		let node: EntryNode | undefined | null = this._canTrimCacheLastNode || tail;
		let lastNode = node;
		let looped = false;
		const startNode = node;
		const canTrimByGid = new Map();

		// TODO only go through heads?
		while (
			node &&
			!(await done()) &&
			values.length > 0 &&
			node &&
			(!looped || node !== startNode)
		) {
			const breakpoint =
				cacheProgress && this._canTrimCacheHashBreakpoint.get(node.value.hash);
			if (breakpoint && node !== tail) {
				// never break on the tail
				break;
			}

			if (!looped || (looped && node !== tail)) {
				lastNode = node;
			}

			let deleteAble = true;

			if (option.filter?.canTrim) {
				deleteAble = canTrimByGid.get(node.value.gid);
				if (deleteAble === undefined) {
					deleteAble = await option.filter?.canTrim(node.value.gid);
					canTrimByGid.set(node.value.gid, deleteAble);
				}

				if (!deleteAble && cacheProgress) {
					// ignore it
					this._canTrimCacheHashBreakpoint.add(node.value.hash, true);
				}
			}
			const prev = node.prev;

			if (deleteAble) {
				// TODO, under some concurrency condition the node can already be removed by another trim process
				const entry = await this._log.deleteNode(node);
				if (entry) {
					deleted.push(entry);
				}
			}

			if (!prev) {
				if (!looped && changed) {
					// pointless to loop around if there are no changes
					node = tail;
					looped = true;
				} else {
					break;
				}
			} else {
				node = prev;
			}
		}

		// remember the node where we started last time from
		this._canTrimCacheLastNode = node || lastNode;
		this._trimLastHead = values.head;
		this._trimLastTail = values.tail;
		this._trimLastOptions = option;
		this._trimLastSeed = seed;

		return deleted;
	}
	/**
	 * @param options
	 * @returns deleted entries
	 */
	async trim(
		option: TrimOptions | undefined = this._trim
	): Promise<Entry<T>[]> {
		const result = await this._queue.add(() => this.trimTask(option));
		if (result instanceof Object) {
			return result;
		}
		throw new Error("Something when wrong when trimming");
	}
}
