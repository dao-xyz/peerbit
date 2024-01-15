import { BlockStore } from "./interface.js";
import { Blocks as IBlocks } from "@peerbit/blocks-interface";
import { DirectStream } from "@peerbit/stream";
import { DirectStreamComponents } from "@peerbit/stream";
import { AnyBlockStore } from "./any-blockstore.js";
import { GetOptions } from "@peerbit/blocks-interface";
import { createStore } from "@peerbit/any-store";
import { BlockMessage, RemoteBlocks } from "./remote.js";
import { PublicSignKey } from "@peerbit/crypto";
import { AnyWhere, DataMessage } from "@peerbit/stream-interface";
import { deserialize, serialize } from "@dao-xyz/borsh";

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
		}
	) {
		super(components, ["/lazyblock/0.0.0"], {
			signaturePolicy: "StrictNoSign",
			messageProcessingConcurrency: options?.messageProcessingConcurrency || 10,
			canRelayMessage: options?.canRelayMessage ?? true,
			connectionManager: {
				dialer: false,
				pruner: false
			}
		});
		this.remoteBlocks = new RemoteBlocks({
			local: new AnyBlockStore(createStore(options?.directory)),
			publish: (message) =>
				this.publish(serialize(message), { mode: new AnyWhere() }),
			localTimeout: options?.localTimeout || 1000,
			messageProcessingConcurrency: options?.messageProcessingConcurrency || 10,
			waitFor: this.waitFor.bind(this)
		});

		this.onDataFn = (data: CustomEvent<DataMessage>) => {
			data.detail?.data?.length &&
				data.detail?.data.length > 0 &&
				this.remoteBlocks.onMessage(
					deserialize(data.detail.data!, BlockMessage)
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
		options?: GetOptions | undefined
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
}
