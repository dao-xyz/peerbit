import { deserialize } from "@dao-xyz/borsh";
import type { PeerId } from "@libp2p/interface";
import { peerIdFromString } from "@libp2p/peer-id";
import {
	type Multiaddr,
	isMultiaddr,
	multiaddr,
} from "@multiformats/multiaddr";
import type {
	Identity,
	PreHash,
	PublicSignKey,
	SignatureWithKey,
} from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import type { Address, OpenOptions, ProgramClient } from "@peerbit/program";
import {
	type CanonicalOpenAdapter,
	type CanonicalOpenMode,
	type CanonicalOpenOptions,
	createManagedProxy,
	getProgramVariant,
} from "./auto.js";
import type { CanonicalChannel } from "./client.js";
import type { CanonicalClient } from "./index.js";

type IdentityProxy = Identity<PublicSignKey> & {
	signer: (
		prehash?: PreHash,
	) => (data: Uint8Array) => Promise<SignatureWithKey>;
};

export type PeerbitCanonicalPeerInfo = {
	peerId: PeerId;
	identity: IdentityProxy;
	multiaddrs: Multiaddr[];
};

const asMultiaddrs = (value: unknown): Multiaddr[] | undefined => {
	if (Array.isArray(value) && value.every((x) => isMultiaddr(x))) {
		return value as Multiaddr[];
	}
	return undefined;
};

export class PeerbitCanonicalClient {
	private _peerId: PeerId;
	private _identity: IdentityProxy;
	private _multiaddrs: Multiaddr[];
	private openState?: {
		adapters: CanonicalOpenAdapter[];
		mode: CanonicalOpenMode;
		adapterCaches: Map<CanonicalOpenAdapter, Map<string, Promise<any>>>;
		proxies: Set<any>;
	};

	private constructor(
		readonly canonical: CanonicalClient,
		info: PeerbitCanonicalPeerInfo,
	) {
		this._peerId = info.peerId;
		this._identity = info.identity;
		this._multiaddrs = info.multiaddrs;
	}

	static async create(
		canonical: CanonicalClient,
		options?: { adapters?: CanonicalOpenAdapter[]; mode?: CanonicalOpenMode },
	): Promise<PeerbitCanonicalClient> {
		await canonical.init();
		const peer = new PeerbitCanonicalClient(canonical, {
			peerId: peerIdFromString(await canonical.peerId()),
			identity: canonical.identity,
			multiaddrs: (await canonical.multiaddrs()).map((a) => multiaddr(a)),
		});
		if (options?.adapters?.length) {
			peer.enableOpen({ adapters: options.adapters, mode: options.mode });
		}
		return peer;
	}

	get peerId(): PeerId {
		return this._peerId;
	}

	get identity(): IdentityProxy {
		return this._identity;
	}

	getMultiaddrs(): Multiaddr[] {
		return this._multiaddrs.slice();
	}

	private static attachParent(
		child: { parents?: any[]; children?: any[] },
		parent?: any,
	) {
		if (child.parents && child.parents.includes(parent) && parent == null) {
			return;
		}
		(child.parents || (child.parents = [])).push(parent);
		if (parent) {
			(parent.children || (parent.children = [])).push(child);
		}
	}

	private async closeOpenProxies(): Promise<void> {
		const state = this.openState;
		if (!state || state.proxies.size === 0) return;
		const targets = [...state.proxies];
		await Promise.allSettled(
			targets.map((proxy) =>
				typeof proxy?.close === "function" ? proxy.close() : undefined,
			),
		);
		state.proxies.clear();
		for (const cache of state.adapterCaches.values()) {
			cache.clear();
		}
	}

	enableOpen(options: {
		adapters: CanonicalOpenAdapter[];
		mode?: CanonicalOpenMode;
	}): void {
		if (!this.openState) {
			this.openState = {
				adapters: [...options.adapters],
				mode: options.mode ?? "canonical",
				adapterCaches: new Map(),
				proxies: new Set(),
			};
			return;
		}
		this.openState.adapters.push(...options.adapters);
		this.openState.mode = options.mode ?? this.openState.mode;
	}

	/**
	 * Refreshes peerId/publicKey/multiaddrs from the canonical host.
	 */
	async refreshPeerInfo(): Promise<PeerbitCanonicalPeerInfo> {
		const info = await this.canonical.peerInfo();
		this._peerId = peerIdFromString(info.peerId);
		this._identity = this.canonical.identity;
		this._multiaddrs = (info.multiaddrs ?? []).map((a) => multiaddr(a));
		return {
			peerId: this._peerId,
			identity: this._identity,
			multiaddrs: this._multiaddrs.slice(),
		};
	}

	async dial(
		address: string | Multiaddr | Multiaddr[] | ProgramClient,
	): Promise<boolean> {
		if (typeof address === "string") {
			return this.canonical.dial(address);
		}
		if (isMultiaddr(address)) {
			return this.canonical.dial(address.toString());
		}
		const many = asMultiaddrs(address);
		if (many) {
			if (many.length === 0) {
				throw new Error("Canonical dial requires at least one multiaddr");
			}
			let lastError: unknown;
			for (const addr of many) {
				try {
					await this.canonical.dial(addr.toString());
					return true;
				} catch (e) {
					lastError = e;
				}
			}
			throw lastError ?? new Error("Canonical dial failed");
		}

		const candidate = address as any;
		if (candidate && typeof candidate.getMultiaddrs === "function") {
			const addrs = await Promise.resolve(candidate.getMultiaddrs());
			return this.dial(addrs as any);
		}

		if (candidate && typeof candidate.toString === "function") {
			return this.canonical.dial(candidate.toString());
		}

		throw new Error("Unsupported dial address");
	}

	async hangUp(
		address: PeerId | PublicSignKey | string | Multiaddr,
	): Promise<void> {
		if (typeof address === "string") {
			await this.canonical.hangUp(address);
			return;
		}
		if (isMultiaddr(address)) {
			await this.canonical.hangUp(address.toString());
			return;
		}

		const candidate = address as any;
		if (candidate && typeof candidate.toPeerId === "function") {
			const peerId = await Promise.resolve(candidate.toPeerId());
			await this.canonical.hangUp(peerId.toString());
			return;
		}

		if (candidate && typeof candidate.toString === "function") {
			await this.canonical.hangUp(candidate.toString());
			return;
		}

		throw new Error("Unsupported hangUp address");
	}

	async start(): Promise<void> {
		await this.canonical.start();
		await this.refreshPeerInfo();
	}

	async stop(): Promise<void> {
		await this.closeOpenProxies();
		await this.canonical.stop();
	}

	async bootstrap(addresses?: string[] | Multiaddr[]): Promise<void> {
		if (!addresses) {
			await this.canonical.bootstrap();
			return;
		}
		const normalized = addresses.map((addr) =>
			typeof addr === "string" ? addr : addr.toString(),
		);
		await this.canonical.bootstrap(normalized);
	}

	openPort(name: string, payload: Uint8Array): Promise<CanonicalChannel> {
		return this.canonical.openPort(name, payload);
	}

	async open<S extends Program<any>>(
		storeOrAddress: S | Address,
		openOptions: CanonicalOpenOptions<S> = {},
	): Promise<S> {
		const state = this.openState;
		if (!state || state.adapters.length === 0) {
			throw new Error(
				"Canonical open is not enabled. Pass adapters to PeerbitCanonicalClient.create(..., { adapters }) or call peer.enableOpen({ adapters }).",
			);
		}

		const mode = openOptions?.mode ?? state.mode;
		if (mode === "local") {
			throw new Error(
				"PeerbitCanonicalClient has no local Peerbit; use mode:'canonical' or omit mode.",
			);
		}

		if (typeof storeOrAddress === "string") {
			const address = storeOrAddress;
			const bytes = await this.canonical.loadProgram(address, {
				timeoutMs: openOptions.timeout,
			});
			const loaded = deserialize(bytes, Program) as Program<any>;
			loaded.address = address;
			return this.open(loaded as any, openOptions as any) as Promise<S>;
		}

		const program = storeOrAddress as Program<any>;
		const programVariant = getProgramVariant(program);
		const adapter = state.adapters.find((candidate) =>
			typeof candidate.canOpen === "function"
				? candidate.canOpen(program)
				: !!programVariant &&
					(candidate.variants ?? (candidate.variant ? [candidate.variant] : []))
						.map(String)
						.includes(programVariant),
		);
		if (!adapter) {
			const knownVariants = state.adapters
				.flatMap(
					(candidate) =>
						candidate.variants ??
						(candidate.variant ? [candidate.variant] : []),
				)
				.filter((x): x is string => typeof x === "string" && x.length > 0);
			throw new Error(
				`No canonical adapter registered for ${program.constructor?.name ?? "program"}${
					programVariant ? ` (variant: '${programVariant}')` : ""
				}${knownVariants.length ? `. Known variants: ${knownVariants.join(", ")}` : ""}`,
			);
		}

		const key = adapter.getKey?.(
			program as any,
			openOptions as OpenOptions<any>,
		);
		if (adapter.getKey && key === undefined) {
			throw new Error(
				`Canonical adapter '${adapter.name}' requires a cache key (adapter.getKey returned undefined)`,
			);
		}
		const existingMode = openOptions?.existing ?? "reject";
		let cache = key ? state.adapterCaches.get(adapter) : undefined;
		if (key && !cache) {
			cache = new Map();
			state.adapterCaches.set(adapter, cache);
		}

		if (key && cache?.has(key)) {
			if (existingMode === "reject") {
				throw new Error(`Program already open for adapter '${adapter.name}'`);
			}
			if (existingMode === "replace") {
				const prev = await cache.get(key);
				if (prev && typeof (prev as any).close === "function") {
					await (prev as any).close();
				}
				cache.delete(key);
			} else {
				const existingProxy = await cache.get(key);
				if (openOptions?.parent) {
					PeerbitCanonicalClient.attachParent(
						existingProxy as any,
						openOptions.parent as any,
					);
				}
				return existingProxy as S;
			}
		}

		const peer = this as any as ProgramClient;
		const openPromise = (async () => {
			const result = await adapter.open({
				program: program as any,
				options: openOptions as OpenOptions<any>,
				peer,
				client: this.canonical,
			});

			let managed: any;
			managed = createManagedProxy(result.proxy as any, {
				address: result.address,
				node: peer,
				onClose: () => {
					if (key && cache?.get(key) === openPromise) {
						cache.delete(key);
					}
					this.openState?.proxies.delete(managed);
				},
			});

			this.openState?.proxies.add(managed);
			if (openOptions?.parent) {
				PeerbitCanonicalClient.attachParent(managed, openOptions.parent as any);
			}
			return managed as S;
		})();

		if (key && cache) {
			cache.set(key, openPromise);
		}
		return openPromise as Promise<S>;
	}

	close(): void {
		void this.closeOpenProxies();
		this.canonical.close();
	}
}
