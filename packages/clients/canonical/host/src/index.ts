import { deserialize, serialize } from "@dao-xyz/borsh";
import { peerIdFromString } from "@libp2p/peer-id";
import {
	CanonicalBootstrapRequest,
	CanonicalConnection,
	CanonicalControlRequest,
	CanonicalControlResponse,
	CanonicalLoadProgramRequest,
	CanonicalSignRequest,
	createMessagePortTransport as createControlTransport,
} from "@peerbit/canonical-transport";
import type {
	CanonicalChannel,
	CanonicalTransport,
} from "@peerbit/canonical-transport";
import { PreHash } from "@peerbit/crypto";
import { type CreateInstanceOptions, Peerbit } from "peerbit";

export type { CanonicalChannel };
export {
	createRpcTransport as createMessagePortTransport,
	type CanonicalRpcTransport,
} from "@peerbit/canonical-transport";

export type CanonicalRuntimeOptions = {
	directory?: string;
	peerOptions?: CreateInstanceOptions;
};

export type CanonicalContext = {
	peer: () => Promise<Peerbit>;
	peerId: () => Promise<string>;
	startPeer?: () => Promise<void>;
	stopPeer?: () => Promise<void>;
};

export type CanonicalHostOptions = {
	idleTimeoutMs?: number;
	idleCheckIntervalMs?: number;
};

export type CanonicalModule = {
	name: string;
	open: (
		ctx: CanonicalContext,
		channel: CanonicalChannel,
		payload: Uint8Array,
	) => void | Promise<void>;
};

export class PeerbitCanonicalRuntime implements CanonicalContext {
	private _peer?: Peerbit;

	constructor(readonly options: CanonicalRuntimeOptions = {}) {}

	async peer(): Promise<Peerbit> {
		if (this._peer) return this._peer;
		const peerOptions = this.options.peerOptions ?? {};
		const directory = this.options.directory ?? peerOptions.directory;
		this._peer = await Peerbit.create({ ...peerOptions, directory });
		await this._peer.start();
		return this._peer;
	}

	async startPeer(): Promise<void> {
		const peer = await this.peer();
		await peer.start();
	}

	async stopPeer(): Promise<void> {
		if (!this._peer) return;
		await this._peer.stop();
		this._peer = undefined;
	}

	async peerId(): Promise<string> {
		return (await this.peer()).peerId.toString();
	}
}

const asUint8Array = (payload: any): Uint8Array => {
	if (payload instanceof Uint8Array) return payload;
	if (payload instanceof ArrayBuffer) return new Uint8Array(payload);
	throw new Error("Expected Uint8Array payload");
};

export class CanonicalHost {
	private readonly modules = new Map<string, CanonicalModule>();
	private nextChannelId = 1;

	constructor(
		readonly ctx: CanonicalContext,
		readonly options: CanonicalHostOptions = {},
	) {}

	registerModule(module: CanonicalModule): void {
		this.modules.set(module.name, module);
	}

	registerModules(modules: CanonicalModule[]): void {
		for (const m of modules) this.registerModule(m);
	}

	attachControlPort(port: MessagePort): () => void {
		return this.attachControlTransport(createControlTransport(port));
	}

	attachControlTransport(transport: CanonicalTransport): () => void {
		const connection = new CanonicalConnection(transport);
		let lastSeen = Date.now();
		const offActivity = connection.onActivity(() => {
			lastSeen = Date.now();
		});
		const unsubscribe = connection.onControl((frame) => {
			if (frame instanceof CanonicalControlRequest) {
				void this.handleControlRequest(connection, frame);
			}
		});
		let idleTimer: ReturnType<typeof setInterval> | undefined;
		const idleTimeoutMs = this.options.idleTimeoutMs;
		const idleIntervalMs =
			this.options.idleCheckIntervalMs ??
			Math.max(1000, Math.min(10_000, Math.floor((idleTimeoutMs ?? 0) / 2)));
		let closed = false;

		const close = () => {
			if (closed) return;
			closed = true;
			offActivity();
			unsubscribe();
			if (idleTimer) clearInterval(idleTimer);
			connection.close();
		};

		if (idleTimeoutMs && idleTimeoutMs > 0) {
			idleTimer = setInterval(() => {
				if (Date.now() - lastSeen > idleTimeoutMs) {
					close();
				}
			}, idleIntervalMs);
		}

		return close;
	}

	private sendResponse(
		connection: CanonicalConnection,
		response: CanonicalControlResponse,
	) {
		connection.sendControl(response);
	}

	private async handleControlRequest(
		connection: CanonicalConnection,
		request: CanonicalControlRequest,
	): Promise<void> {
		const id = request.id;
		try {
			if (request.op === "peerId") {
				const peerId = await this.ctx.peerId();
				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true, peerId }),
				);
				return;
			}

			if (request.op === "peerInfo") {
				const peer = await this.ctx.peer();
				const peerId = await this.ctx.peerId();
				const publicKey = peer.identity.publicKey.bytes;
				const strings = peer.getMultiaddrs().map((x) => x.toString());
				this.sendResponse(
					connection,
					new CanonicalControlResponse({
						id,
						ok: true,
						peerId,
						payload: publicKey,
						strings,
					}),
				);
				return;
			}

			if (request.op === "dial") {
				const address = String(request.name ?? "");
				if (!address) {
					throw new Error("Canonical dial requires request.name to be set");
				}
				const peer = await this.ctx.peer();
				await peer.dial(address);
				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true }),
				);
				return;
			}

			if (request.op === "start") {
				if (typeof (this.ctx as any).startPeer === "function") {
					await (this.ctx as any).startPeer();
				} else {
					const peer = await this.ctx.peer();
					await peer.start();
				}
				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true }),
				);
				return;
			}

			if (request.op === "stop") {
				if (typeof (this.ctx as any).stopPeer === "function") {
					await (this.ctx as any).stopPeer();
				} else {
					const peer = await this.ctx.peer();
					await peer.stop();
				}
				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true }),
				);
				return;
			}

			if (request.op === "bootstrap") {
				let addresses: string[] | undefined;
				if (request.payload) {
					const parsed = deserialize(
						request.payload,
						CanonicalBootstrapRequest,
					) as CanonicalBootstrapRequest;
					addresses = parsed.addresses?.length ? parsed.addresses : undefined;
				}
				const peer = await this.ctx.peer();
				await peer.bootstrap(addresses);
				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true }),
				);
				return;
			}

			if (request.op === "loadProgram") {
				const address = String(request.name ?? "");
				if (!address) {
					throw new Error(
						"Canonical loadProgram requires request.name to be set",
					);
				}
				let timeoutMs: number | undefined;
				if (request.payload) {
					const parsed = deserialize(
						request.payload,
						CanonicalLoadProgramRequest,
					) as CanonicalLoadProgramRequest;
					timeoutMs = parsed.timeoutMs;
				}

				const peer = await this.ctx.peer();
				const bytes = await peer.services.blocks.get(address, {
					remote: { timeout: timeoutMs },
				});
				if (!bytes) {
					this.sendResponse(
						connection,
						new CanonicalControlResponse({
							id,
							ok: false,
							error: "Program not found",
						}),
					);
					return;
				}

				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true, payload: bytes }),
				);
				return;
			}

			if (request.op === "hangUp") {
				const address = String(request.name ?? "");
				if (!address) {
					throw new Error("Canonical hangUp requires request.name to be set");
				}
				const peer = await this.ctx.peer();
				try {
					await peer.hangUp(address);
				} catch (e) {
					try {
						await peer.hangUp(peerIdFromString(address));
					} catch {
						throw e;
					}
				}
				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true }),
				);
				return;
			}

			if (request.op === "sign") {
				if (!request.payload) {
					throw new Error("Canonical sign requires request.payload to be set");
				}
				const signRequest = deserialize(
					request.payload,
					CanonicalSignRequest,
				) as CanonicalSignRequest;
				const peer = await this.ctx.peer();
				const prehash =
					signRequest.prehash != null
						? (signRequest.prehash as PreHash)
						: PreHash.NONE;
				const signature = await peer.identity.sign(signRequest.data, prehash);
				this.sendResponse(
					connection,
					new CanonicalControlResponse({
						id,
						ok: true,
						payload: serialize(signature),
					}),
				);
				return;
			}

			if (request.op === "ping") {
				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true }),
				);
				return;
			}

			if (request.op === "open") {
				const name = String(request.name ?? "");
				const mod = this.modules.get(name);
				if (!mod) {
					this.sendResponse(
						connection,
						new CanonicalControlResponse({
							id,
							ok: false,
							error: `Unknown module '${name}'`,
						}),
					);
					return;
				}

				const payload =
					request.payload != null
						? asUint8Array(request.payload)
						: new Uint8Array();
				const channelId = this.nextChannelId++;
				const channel = connection.createChannel(channelId);
				try {
					await mod.open(this.ctx, channel, payload);
				} catch (e: any) {
					connection.releaseChannel(channelId);
					throw e;
				}

				this.sendResponse(
					connection,
					new CanonicalControlResponse({ id, ok: true, channelId }),
				);
				return;
			}

			this.sendResponse(
				connection,
				new CanonicalControlResponse({
					id,
					ok: false,
					error: `Unknown op '${String(request.op)}'`,
				}),
			);
		} catch (e: any) {
			this.sendResponse(
				connection,
				new CanonicalControlResponse({
					id,
					ok: false,
					error: String(e?.message || e),
				}),
			);
		}
	}
}
