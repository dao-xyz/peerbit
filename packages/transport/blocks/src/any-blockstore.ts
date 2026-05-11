import { createStore } from "@peerbit/any-store";
import { type AnyStore } from "@peerbit/any-store-interface";
import {
	type Blocks,
	type GetOptions,
	calculateRawCid,
	cidifyString,
	codecCodes,
	defaultHasher,
} from "@peerbit/blocks-interface";
import type {
	PeerRefs,
	WaitForAnyOpts,
	WaitForPresentOpts,
} from "@peerbit/stream-interface";
import { waitFor } from "@peerbit/time";
import { type Block, decode } from "multiformats/block";
import * as raw from "multiformats/codecs/raw";

export class AnyBlockStore implements Blocks {
	private _store: AnyStore;
	private _opening: Promise<any>;
	private _onClose: (() => any) | undefined;
	private _closeController?: AbortController;
	constructor(store: AnyStore = createStore()) {
		this._store = store;
	}

	private async decodeStoredBytes(
		cid: string,
		bytes: Uint8Array,
		options?: { hasher?: any },
	): Promise<Uint8Array> {
		const cidObject = cidifyString(cid);
		if (
			cidObject.code === raw.code &&
			(options?.hasher == null || options.hasher === defaultHasher)
		) {
			return bytes;
		}
		const codec = (codecCodes as any)[cidObject.code];
		const block = await decode({
			bytes,
			codec,
			hasher: options?.hasher || defaultHasher,
		});
		return (block as Block<Uint8Array, any, any, any>).bytes;
	}

	async get(
		cid: string,
		options?: {
			raw?: boolean;
			links?: string[];
			hasher?: any;
			remote: {
				timeout?: number;
			};
		},
	): Promise<Uint8Array | undefined> {
		try {
			const bytes = await this._store.get(cid);
			if (!bytes) {
				return undefined;
			}
			return this.decodeStoredBytes(cid, bytes, options);
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

	async getMany(
		cids: string[],
		options?: GetOptions & { hasher?: any },
	): Promise<Array<Uint8Array | undefined>> {
		const store = this._store as AnyStore & {
			getMany?: (keys: string[]) => Promise<Array<Uint8Array | undefined>>;
		};
		try {
			if (typeof store.getMany === "function") {
				const values = await store.getMany(cids);
				return Promise.all(
					values.map((bytes, index) =>
						bytes
							? this.decodeStoredBytes(cids[index]!, bytes, options)
							: undefined,
					),
				);
			}
			return Promise.all(cids.map((cid) => this.get(cid, options as any)));
		} catch (error: any) {
			if (
				typeof error?.code === "string" &&
				error?.code?.indexOf("LEVEL_NOT_FOUND") !== -1
			) {
				return Promise.all(cids.map((cid) => this.get(cid, options as any)));
			}
			throw error;
		}
	}

	async put(
		bytes: Uint8Array | { block: Block<any, any, any, any>; cid: string },
	): Promise<string> {
		const put =
			bytes instanceof Uint8Array ? await calculateRawCid(bytes) : bytes;
		const bbytes = put.block.bytes;
		try {
			await this._store.put(put.cid, bbytes);
		} catch (error: any) {
			if (await this.isClosingStorePutError(error)) {
				// Late replication writes can outlive shutdown. At this point the
				// backing store is intentionally closing, so report the deterministic
				// CID while discarding the write instead of leaking an unhandled
				// LevelDB rejection.
				return put.cid;
			}
			throw error;
		}
		return put.cid;
	}

	async putMany(
		blocks: Array<Uint8Array | { block: Block<any, any, any, any>; cid: string }>,
	): Promise<string[]> {
		const puts = await Promise.all(
			blocks.map((bytes) =>
				bytes instanceof Uint8Array ? calculateRawCid(bytes) : bytes,
			),
		);
		const store = this._store as AnyStore & {
			putMany?: (entries: Iterable<readonly [string, Uint8Array]>) => Promise<void>;
		};
		try {
			if (typeof store.putMany === "function") {
				await store.putMany(puts.map((put) => [put.cid, put.block.bytes] as const));
			} else {
				for (const put of puts) {
					await this._store.put(put.cid, put.block.bytes);
				}
			}
		} catch (error: any) {
			if (await this.isClosingStorePutError(error)) {
				return puts.map((put) => put.cid);
			}
			throw error;
		}
		return puts.map((put) => put.cid);
	}

	private async isClosingStorePutError(error: any): Promise<boolean> {
		const status = await this._store.status();
		return (
			typeof error?.code === "string" &&
			error.code === "LEVEL_DATABASE_NOT_OPEN" &&
			this._closeController?.signal.aborted === true &&
			(status === "closing" || status === "closed")
		);
	}

	async rm(cid: string): Promise<void> {
		await this._store.del(cid);
	}

	async rmMany(cids: string[]): Promise<number> {
		const store = this._store as AnyStore & {
			delMany?: (keys: string[]) => Promise<number>;
		};
		if (typeof store.delMany === "function") {
			return store.delMany(cids);
		}
		await Promise.all(cids.map((cid) => this._store.del(cid)));
		return cids.length;
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const [key, value] of this._store.iterator()) {
			yield [key, value];
		}
	}

	async has(cid: string) {
		try {
			return !!(await this._store.get(cid));
		} catch (error: any) {
			if (
				typeof error?.code === "string" &&
				error?.code?.indexOf("LEVEL_NOT_FOUND") !== -1
			) {
				return false;
			}
			throw error;
		}
	}

	async hasMany(cids: string[]): Promise<boolean[]> {
		const store = this._store as AnyStore & {
			getMany?: (
				keys: string[],
			) => Promise<Array<Uint8Array | undefined>> | Array<Uint8Array | undefined>;
			hasMany?: (keys: string[]) => Promise<boolean[]> | boolean[];
		};
		if (typeof store.hasMany === "function") {
			return store.hasMany(cids);
		}
		if (typeof store.getMany === "function") {
			const values = await store.getMany(cids);
			return values.map((value) => value != null);
		}

		return Promise.all(cids.map((cid) => this.has(cid)));
	}

	async start(): Promise<void> {
		await this._store.open();
		this._closeController = new AbortController();

		try {
			this._opening = waitFor(
				async () => (await this._store.status()) === "open",
				{
					delayInterval: 100,
					timeout: 10 * 1000,
					signal: this._closeController.signal,
				},
			);
			await this._opening;
		} finally {
			this._onClose = undefined;
		}
	}

	async stop(): Promise<void> {
		this._onClose?.();
		this._closeController?.abort();
		return this._store.close();
	}

	status() {
		return this._store.status();
	}
	async waitFor(
		peer: PeerRefs,
		options?: WaitForPresentOpts | WaitForAnyOpts,
	): Promise<string[]> {
		// Offline storage // TODO this feels off resolving
		return [];
	}

	async size() {
		return this._store.size();
	}

	persisted(): boolean | Promise<boolean> {
		return this._store.persisted();
	}
}
