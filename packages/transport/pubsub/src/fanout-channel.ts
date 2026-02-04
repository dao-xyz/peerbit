import type { FanoutTree, FanoutTreeChannelOptions, FanoutTreeDataEvent, FanoutTreeJoinOptions } from "./fanout-tree.js";

export interface FanoutChannelEvents {
	data: CustomEvent<FanoutTreeDataEvent>;
	joined: CustomEvent<{ topic: string; root: string; parent: string }>;
	kicked: CustomEvent<{ topic: string; root: string; from: string }>;
}

export class FanoutChannel extends EventTarget {
	public readonly root: string;
	public readonly topic: string;

	private listening = false;
	private readonly onData = (ev: any) => {
		const d = ev?.detail as FanoutTreeDataEvent | undefined;
		if (!d) return;
		if (d.root !== this.root) return;
		if (d.topic !== this.topic) return;
		this.dispatchEvent(new CustomEvent("data", { detail: d }));
	};
	private readonly onJoined = (ev: any) => {
		const d = ev?.detail as { topic: string; root: string; parent: string } | undefined;
		if (!d) return;
		if (d.root !== this.root) return;
		if (d.topic !== this.topic) return;
		this.dispatchEvent(new CustomEvent("joined", { detail: d }));
	};
	private readonly onKicked = (ev: any) => {
		const d = ev?.detail as { topic: string; root: string; from: string } | undefined;
		if (!d) return;
		if (d.root !== this.root) return;
		if (d.topic !== this.topic) return;
		this.dispatchEvent(new CustomEvent("kicked", { detail: d }));
	};

	constructor(
		private readonly fanout: FanoutTree,
		opts: { topic: string; root: string },
	) {
		super();
		this.topic = opts.topic;
		this.root = opts.root;
	}

	static fromSelf(fanout: FanoutTree, topic: string): FanoutChannel {
		return new FanoutChannel(fanout, { topic, root: fanout.publicKeyHash });
	}

	private ensureListening() {
		if (this.listening) return;
		this.listening = true;
		this.fanout.addEventListener("fanout:data", this.onData);
		this.fanout.addEventListener("fanout:joined", this.onJoined);
		this.fanout.addEventListener("fanout:kicked", this.onKicked);
	}

	public close() {
		if (!this.listening) return;
		this.listening = false;
		this.fanout.removeEventListener("fanout:data", this.onData);
		this.fanout.removeEventListener("fanout:joined", this.onJoined);
		this.fanout.removeEventListener("fanout:kicked", this.onKicked);
	}

	public openAsRoot(options: Omit<FanoutTreeChannelOptions, "role">) {
		this.ensureListening();
		return this.fanout.openChannel(this.topic, this.root, { ...options, role: "root" });
	}

	public join(
		options: Omit<FanoutTreeChannelOptions, "role">,
		joinOpts?: FanoutTreeJoinOptions,
	) {
		this.ensureListening();
		return this.fanout.joinChannel(this.topic, this.root, options, joinOpts);
	}

	public publish(payload: Uint8Array) {
		return this.fanout.publishData(this.topic, this.root, payload);
	}

	public end(lastSeqExclusive: number) {
		return this.fanout.publishEnd(this.topic, this.root, lastSeqExclusive);
	}
}

