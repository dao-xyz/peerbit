import type {
	FanoutTree,
	FanoutTreeChannelOptions,
	FanoutTreeDataEvent,
	FanoutTreeJoinOptions,
	FanoutTreeUnicastEvent,
} from "./fanout-tree.js";

export interface FanoutChannelEvents {
	data: CustomEvent<FanoutTreeDataEvent>;
	unicast: CustomEvent<FanoutTreeUnicastEvent>;
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
	private readonly onUnicast = (ev: any) => {
		const d = ev?.detail as FanoutTreeUnicastEvent | undefined;
		if (!d) return;
		if (d.root !== this.root) return;
		if (d.topic !== this.topic) return;
		this.dispatchEvent(new CustomEvent("unicast", { detail: d }));
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
		this.fanout.addEventListener("fanout:unicast", this.onUnicast);
		this.fanout.addEventListener("fanout:joined", this.onJoined);
		this.fanout.addEventListener("fanout:kicked", this.onKicked);
	}

	public close() {
		if (!this.listening) return;
		this.listening = false;
		this.fanout.removeEventListener("fanout:data", this.onData);
		this.fanout.removeEventListener("fanout:unicast", this.onUnicast);
		this.fanout.removeEventListener("fanout:joined", this.onJoined);
		this.fanout.removeEventListener("fanout:kicked", this.onKicked);
	}

	public async leave(options?: { notifyParent?: boolean; kickChildren?: boolean }) {
		this.close();
		await this.fanout.closeChannel(this.topic, this.root, options);
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
		return this.fanout.publishToChannel(this.topic, this.root, payload);
	}

	public getPeerHashes(options?: { includeSelf?: boolean }) {
		return this.fanout.getChannelPeerHashes(this.topic, this.root, options);
	}

	public getRouteToken() {
		return this.fanout.getRouteToken(this.topic, this.root);
	}

	public resolveRouteToken(
		targetHash: string,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	) {
		return this.fanout.resolveRouteToken(this.topic, this.root, targetHash, options);
	}

	public unicast(toRoute: string[], payload: Uint8Array) {
		return this.fanout.unicast(this.topic, this.root, toRoute, payload);
	}

	public unicastTo(
		targetHash: string,
		payload: Uint8Array,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	) {
		return this.fanout.unicastTo(this.topic, this.root, targetHash, payload, options);
	}

	public unicastAck(
		toRoute: string[],
		payload: Uint8Array,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	) {
		const target = toRoute[toRoute.length - 1]!;
		return this.fanout.unicastAck(this.topic, this.root, toRoute, target, payload, options);
	}

	public unicastToAck(
		targetHash: string,
		payload: Uint8Array,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	) {
		return this.fanout.unicastToAck(this.topic, this.root, targetHash, payload, options);
	}

	public end(lastSeqExclusive: number) {
		return this.fanout.publishEnd(this.topic, this.root, lastSeqExclusive);
	}
}
