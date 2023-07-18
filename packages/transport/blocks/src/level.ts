import { Blocks } from "@peerbit/blocks-interface";
import {
	cidifyString,
	codecCodes,
	createBlock,
	defaultHasher,
	stringifyCid,
} from "./block.js";
import * as Block from "multiformats/block";
import { AbstractLevel } from "abstract-level";
import { MemoryLevel } from "memory-level";
import { waitFor } from "@peerbit/time";
import LazyLevel, { LazyLevelOptions } from "@peerbit/lazy-level";
import { PutOptions } from "@peerbit/blocks-interface";
import { PeerId } from "@libp2p/interface-peer-id";
import { PublicSignKey } from "@peerbit/crypto";

export class LevelBlockStore implements Blocks {
	private _level: LazyLevel;
	private _opening: Promise<any>;
	private _closed = false;
	private _onClose: (() => any) | undefined;
	constructor(
		level: AbstractLevel<any, string, Uint8Array>,
		options?: LazyLevelOptions
	) {
		this._level = new LazyLevel(level, options);
	}

	async get(
		cid: string,
		options?: {
			raw?: boolean;
			links?: string[];
			timeout?: number;
			hasher?: any;
		}
	): Promise<Uint8Array | undefined> {
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
			return (block as Block.Block<Uint8Array, any, any, any>).bytes;
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

	async put(bytes: Uint8Array): Promise<string> {
		const block = await createBlock(bytes, "raw");
		const cid = stringifyCid(block.cid);
		const bbytes = block.bytes;
		await this._level.put(cid, bbytes);
		return cid;
	}

	async rm(cid: string): Promise<void> {
		await this._level.del(cid);
	}

	async has(cid: string) {
		return !!(await this._level.get(cid));
	}

	async start(): Promise<void> {
		this._closed = false;
		await this._level.open();

		try {
			this._opening = waitFor(() => this._level.status() === "open", {
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
	}

	async stop(): Promise<void> {
		await this.idle();
		this._closed = true;
		this._onClose && this._onClose();
		return this._level.close();
	}

	async idle(): Promise<void> {
		await this._level.idle();
	}

	status() {
		return this._level.status();
	}
	async waitFor(peer: PeerId | PublicSignKey): Promise<void> {
		return; // Offline storage // TODO this feels off resolving
	}
}

export class MemoryLevelBlockStore extends LevelBlockStore {
	constructor(options?: LazyLevelOptions) {
		super(new MemoryLevel({ valueEncoding: "view" }), options);
	}
}
