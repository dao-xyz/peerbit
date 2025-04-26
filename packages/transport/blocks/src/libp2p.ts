import { deserialize, serialize } from "@dao-xyz/borsh";
import { createStore } from "@peerbit/any-store";
import type { GetOptions, Blocks as IBlocks } from "@peerbit/blocks-interface";
import { type PublicSignKey } from "@peerbit/crypto";
import { DirectStream } from "@peerbit/stream";
import { type DirectStreamComponents } from "@peerbit/stream";
import { type DataMessage } from "@peerbit/stream-interface";
import { AnyBlockStore } from "./any-blockstore.js";
import { BlockMessage, RemoteBlocks } from "./remote.js";

export type DirectBlockComponents = DirectStreamComponents;

export class DirectBlock extends DirectStream implements IBlocks {
	private remoteBlocks: RemoteBlocks;
	private onDataFn: any;
	private onPeerConnectedFn: any;

	constructor(
		components: DirectBlockComponents,
		options?: {
			directory?: string;
			canRelayMessage?: boolean;
			localTimeout?: number;
			messageProcessingConcurrency?: number;
			eagerBlocks?: boolean | { cacheSize?: number };
		},
	) {
		super(components, ["/lazyblock/0.0.0"], {
			messageProcessingConcurrency: options?.messageProcessingConcurrency || 10,
			canRelayMessage: options?.canRelayMessage ?? true,
			connectionManager: {
				dialer: false,
				pruner: false,
			},
		});
		this.remoteBlocks = new RemoteBlocks({
			local: new AnyBlockStore(createStore(options?.directory)),
			publish: (message, options) => this.publish(serialize(message), options),
			localTimeout: options?.localTimeout || 1000,
			messageProcessingConcurrency: options?.messageProcessingConcurrency || 10,
			waitFor: this.waitFor.bind(this),
			publicKey: this.publicKey,
			eagerBlocks: options?.eagerBlocks,
		});

		this.onDataFn = (data: CustomEvent<DataMessage>) => {
			data.detail?.data?.length &&
				data.detail?.data.length > 0 &&
				this.remoteBlocks.onMessage(
					deserialize(data.detail.data!, BlockMessage),
					data.detail.header.signatures?.publicKeys[0]?.hashcode(),
				);
		};
		this.onPeerConnectedFn = (evt: CustomEvent<PublicSignKey>) =>
			this.remoteBlocks.onReachable(evt.detail);
	}

	async put(bytes: Uint8Array): Promise<string> {
		return this.remoteBlocks.put(bytes);
	}

	async has(cid: string) {
		return this.remoteBlocks.has(cid);
	}
	async get(
		cid: string,
		options?: GetOptions | undefined,
	): Promise<Uint8Array | undefined> {
		return this.remoteBlocks.get(cid, options);
	}

	async rm(cid: string) {
		return this.remoteBlocks.rm(cid);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const [key, value] of this.remoteBlocks.iterator()) {
			yield [key, value];
		}
	}

	async start(): Promise<void> {
		this.addEventListener("data", this.onDataFn);
		this.addEventListener("peer:reachable", this.onPeerConnectedFn);
		await super.start();
		await this.remoteBlocks.start();
	}

	async stop(): Promise<void> {
		this.removeEventListener("data", this.onDataFn);
		this.removeEventListener("peer:reachable", this.onPeerConnectedFn);
		await super.stop();
		await this.remoteBlocks.stop();
	}

	async size() {
		return this.remoteBlocks?.size() || 0;
	}
	get status() {
		return this.remoteBlocks?.status || this.started;
	}

	persisted(): boolean | Promise<boolean> {
		return this.remoteBlocks.persisted();
	}
}
