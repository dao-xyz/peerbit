import { createStore } from "@peerbit/any-store";
import { type AnyStore } from "@peerbit/any-store-interface";
import {
	type Blocks,
	calculateRawCid,
	cidifyString,
	codecCodes,
	defaultHasher,
} from "@peerbit/blocks-interface";
import type {
	WaitForAnyOpts,
	WaitForPresentOpts,
} from "@peerbit/stream-interface";
import type { PeerRefs } from "@peerbit/stream-interface";
import { waitFor } from "@peerbit/time";
import { type Block, decode } from "multiformats/block";

export class AnyBlockStore implements Blocks {
	private _store: AnyStore;
	private _opening: Promise<any>;
	private _onClose: (() => any) | undefined;
	private _closeController: AbortController;
	constructor(store: AnyStore = createStore()) {
		this._store = store;
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
		const cidObject = cidifyString(cid);
		try {
			const bytes = await this._store.get(cid);
			if (!bytes) {
				return undefined;
			}
			const codec = (codecCodes as any)[cidObject.code];
			const block = await decode({
				bytes,
				codec,
				hasher: options?.hasher || defaultHasher,
			});
			return (block as Block<Uint8Array, any, any, any>).bytes;
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

	async put(
		bytes: Uint8Array | { block: Block<any, any, any, any>; cid: string },
	): Promise<string> {
		const put =
			bytes instanceof Uint8Array ? await calculateRawCid(bytes) : bytes;
		const bbytes = put.block.bytes;
		await this._store.put(put.cid, bbytes);
		return put.cid;
	}

	async rm(cid: string): Promise<void> {
		await this._store.del(cid);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const [key, value] of this._store.iterator()) {
			yield [key, value];
		}
	}

	async has(cid: string) {
		return !!(await this._store.get(cid));
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
