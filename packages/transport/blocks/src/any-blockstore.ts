import { type PeerId } from "@libp2p/interface";
import { createStore } from "@peerbit/any-store";
import { type AnyStore } from "@peerbit/any-store-interface";
import {
	type Blocks,
	cidifyString,
	codecCodes,
	createBlock,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { type PublicSignKey } from "@peerbit/crypto";
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

	async put(bytes: Uint8Array): Promise<string> {
		const block = await createBlock(bytes, "raw");
		const cid = stringifyCid(block.cid);
		const bbytes = block.bytes;
		await this._store.put(cid, bbytes);
		return cid;
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
	async waitFor(peer: PeerId | PublicSignKey): Promise<void> {
		// Offline storage // TODO this feels off resolving
	}

	async size() {
		return this._store.size();
	}

	persisted(): boolean | Promise<boolean> {
		return this._store.persisted();
	}
}
