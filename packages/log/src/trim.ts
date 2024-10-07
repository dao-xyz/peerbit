import { Cache } from "@peerbit/cache";
import PQueue from "p-queue";
import type { EntryIndex } from "./entry-index.js";
import type { ShallowEntry } from "./entry-shallow.js";
import type { Entry } from "./entry.js";
import type { SortFn } from "./log-sorting.js";

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
		canTrim: (entry: ShallowEntry) => Promise<boolean> | boolean;
		cacheId?: () => string | number;
	};
};
export type TrimOptions = TrimCanAppendOption & TrimCondition;

interface Log<T> {
	index: EntryIndex<T>;
	sortFn: SortFn;
	deleteNode: (node: ShallowEntry) => Promise<Entry<T> | undefined>;
	getLength(): number;
}
export class Trim<T> {
	private _trim?: TrimOptions;
	private _canTrimCacheLastNode: ShallowEntry | undefined | null;
	private _trimLastHead: ShallowEntry | undefined | null;
	private _trimLastTail: ShallowEntry | undefined | null;
	private _trimLastLength = 0;

	private _trimLastOptions?: TrimOptions;
	private _trimLastSeed: string | number | undefined;
	private _canTrimCacheHashBreakpoint: Cache<boolean>;
	private _log: Log<T>;
	private _queue: PQueue;
	constructor(log: Log<T>, options?: TrimOptions) {
		this._log = log;
		this._trim = options;
		this._canTrimCacheHashBreakpoint = new Cache({ max: 1e5 });
		this._queue = new PQueue({ concurrency: 1 });
	}

	async deleteFromCache(hash: string) {
		if (this._canTrimCacheLastNode?.hash === hash) {
			// we do 'getAfter' here, because earlier entries might already have been deleted or checked for deletion but not removed due to some filtering logic
			this._canTrimCacheLastNode = await this._log.index.getAfter(
				this._canTrimCacheLastNode,
				false,
			);
		}
	}

	get options() {
		return this._trim;
	}

	private async trimTask(
		option: TrimOptions | undefined = this._trim,
	): Promise<Entry<T>[]> {
		if (!option) {
			return [];
		}
		///  TODO Make this method less ugly
		const deleted: Entry<T>[] = [];

		let done: () => Promise<boolean> | boolean;
		/* 		const valueIterator = this._log.index.query([], this._log.sortFn.sort, false); */
		if (option.type === "length") {
			const to = option.to;
			const from = option.from ?? to;
			if (this._log.getLength() < from) {
				return [];
			}
			done = async () => this._log.getLength() <= to;
		} else if (option.type === "bytelength") {
			// TODO calculate the sum and cache it and update it only when entries are added or removed
			const byteLengthFn = async () =>
				BigInt(
					await this._log.index.properties.index.sum({ key: "payloadSize" }),
				);

			// prune to max sum payload sizes in bytes
			const byteLengthFrom = BigInt(option.from ?? option.to);

			if ((await byteLengthFn()) < byteLengthFrom) {
				return [];
			}
			done = async () => (await byteLengthFn()) <= option.to;
		} else if (option.type === "time") {
			const s0 = BigInt(+new Date() * 1e6);
			const maxAge = option.maxAge * 1e6;
			done = async () => {
				if (!(await this._log.index.getOldest())) {
					return true;
				}

				const nodeValue = await this._log.index.getOldest();

				if (!nodeValue) {
					return true;
				}

				return s0 - nodeValue.meta.clock.timestamp.wallTime < maxAge;
			};
		} else {
			return [];
		}

		const tail = await this._log.index.getOldest(false);

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

			changed =
				this._trimLastHead?.hash !==
					(await this._log.index.getNewest())?.hash ||
				this._trimLastTail?.hash !==
					(await this._log.index.getOldest())?.hash ||
				this._trimLastLength !== this._log.getLength() ||
				trimOptionsChanged;

			if (!changed) {
				return [];
			}
		}

		let node: ShallowEntry | undefined | null =
			this._canTrimCacheLastNode || tail; // TODO should we do this._canTrimCacheLastNode?.prev instead ?
		let lastNode: ShallowEntry | undefined | null = node;
		let looped = false;
		const startNode = node;
		let canTrimByGid: Map<string, boolean> | undefined = undefined;

		// TODO only go through heads?
		while (
			node &&
			!(await done()) &&
			this._log.getLength() > 0 &&
			node &&
			(!looped || node.hash !== startNode?.hash)
		) {
			let deleteAble: boolean | undefined = true;
			if (option.filter?.canTrim) {
				canTrimByGid = canTrimByGid || new Map();
				if (!node) {
					throw new Error("Unexpected missing entry when trimming: " + node);
				}

				deleteAble = canTrimByGid.get(node.meta.gid);
				if (deleteAble === undefined) {
					deleteAble = await option.filter?.canTrim(node);
					canTrimByGid.set(node.meta.gid, deleteAble);
				}

				if (!deleteAble && cacheProgress) {
					// ignore it
					this._canTrimCacheHashBreakpoint.add(node.hash, true);
				}
			}

			// Delete, and update current node
			if (deleteAble) {
				// Do this before deleteNode, else prev/next might be gone

				const prev: ShallowEntry | undefined = await this._log.index.getAfter(
					node,
					false,
				);
				const next: ShallowEntry | undefined = await this._log.index.getBefore(
					node,
					false,
				);

				const entry = await this._log.deleteNode(node);
				if (entry) {
					deleted.push(entry);
				}

				node = prev;
				// If we don't do this, we might, next time start to iterate from a node that does not exist
				// we do prev 'or' next because next time we want to start as close as possible to where we left of
				lastNode = prev || next;
			} else {
				lastNode = node;
				node = await this._log.index.getAfter(node, false);
			}

			if (!node) {
				if (!looped && changed && !cacheProgress) {
					node = tail;
					looped = true;
				} else {
					break;
				}
			}
		}

		// remember the node where we started last time from
		this._canTrimCacheLastNode = node || lastNode;
		this._trimLastHead = await this._log.index.getNewest();
		this._trimLastTail = await this._log.index.getOldest();
		this._trimLastLength = this._log.getLength();
		this._trimLastOptions = option;
		this._trimLastSeed = seed;

		return deleted;
	}
	/**
	 * @param options
	 * @returns deleted entries
	 */
	async trim(
		options: TrimOptions | undefined = this._trim,
	): Promise<Entry<T>[] | undefined> {
		if (!options) {
			return;
		}
		const result = await this._queue.add(() => this.trimTask(options));
		if (result instanceof Object) {
			return result;
		}
		throw new Error("Something when wrong when trimming");
	}
}
