import { BlockStore, PutOptions } from "./store.js";
import {
	cidifyString,
	codecCodes,
	defaultHasher,
	stringifyCid,
} from "./block.js";
import * as Block from "multiformats/block";
import { AbstractLevel } from "abstract-level";
import { MemoryLevel } from "memory-level";
import { waitFor } from "@dao-xyz/peerbit-time";
import LazyLevel, { LazyLevelOptions } from "@dao-xyz/lazy-level";

export class LevelBlockStore implements BlockStore {
	_level: LazyLevel;
	_opening: Promise<any>;
	_closed = false;
	_onClose: (() => any) | undefined;
	constructor(
		level: AbstractLevel<any, string, Uint8Array>,
		options?: LazyLevelOptions
	) {
		this._level = new LazyLevel(level, options);
	}

	async get<T>(
		cid: string,
		options?: {
			raw?: boolean;
			links?: string[];
			timeout?: number;
			hasher?: any;
		}
	): Promise<Block.Block<T, any, any, any> | undefined> {
		const cidObject = cidifyString(cid);
		try {
			const bytes = await this._level.get(cid);
			if (!bytes) {
				return undefined;
			}
			const codec = codecCodes[cidObject.code];
			const block = await Block.decode({
				bytes,
				codec,
				hasher: options?.hasher || defaultHasher,
			});
			return block as Block.Block<T, any, any, any>;
		} catch (error: any) {
			if (
				typeof error?.code === "string" &&
				error?.code?.indexOf("LEVEL_NOT_FOUND") !== -1
			) {
				return undefined;
			}
			throw error;
		}
	}

	async put<T>(
		block: Block.Block<T, any, any, any>,
		options?: PutOptions
	): Promise<string> {
		const cid = stringifyCid(block.cid);
		const bytes = block.bytes;
		await this._level.set(cid, bytes);
		return cid;
	}

	async rm(cid: string): Promise<void> {
		await this._level.del(cid);
	}

	async open(): Promise<this> {
		this._closed = false;
		await this._level.open();

		try {
			this._opening = waitFor(() => this._level.status === "open", {
				delayInterval: 100,
				timeout: 10 * 1000,
				stopperCallback: (fn) => {
					this._onClose = fn;
				},
			});
			await this._opening;
		} finally {
			this._onClose = undefined;
		}
		return this;
	}

	async close(): Promise<void> {
		await this.idle();
		this._closed = true;
		this._onClose && this._onClose();
		return this._level.close();
	}

	async idle(): Promise<void> {
		await this._level.idle();
	}

	get status() {
		return this._level.status;
	}
}

export class MemoryLevelBlockStore extends LevelBlockStore {
	constructor(options?: LazyLevelOptions) {
		super(new MemoryLevel({ valueEncoding: "view" }), options);
	}
}
