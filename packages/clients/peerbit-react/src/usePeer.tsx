import type { PeerId } from "@libp2p/interface";
import type { Multiaddr } from "@multiformats/multiaddr";
import type {
	CanonicalClient,
	CanonicalOpenAdapter,
	CanonicalOpenMode,
	ConnectServiceWorkerOptions,
	ConnectWindowOptions,
} from "@peerbit/canonical-client";
import type { Identity, PublicSignKey } from "@peerbit/crypto";
import { Ed25519Keypair } from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import { logger as createLogger } from "@peerbit/logger";
import type { Address, OpenOptions, Program } from "@peerbit/program";
import { waitFor } from "@peerbit/time";
import * as React from "react";
import type { JSX } from "react";
import { v4 as uuid } from "uuid";
import { FastMutex } from "./lockstorage.ts";
import {
	cookiesWhereClearedJustNow,
	getClientId,
	getFreeKeypair,
	inIframe,
} from "./utils.ts";

const log = createLogger("peerbit:react:usePeer");
const singletonLog = log.newScope("singleton");
const keypairLog = log.newScope("keypair");
const storageLog = log.newScope("storage");
const clientLog = log.newScope("client");
const bootstrapLog = log.newScope("bootstrap");

const isInStandaloneMode = () =>
	window.matchMedia("(display-mode: standalone)").matches ||
	((window.navigator as unknown as Record<string, unknown>)["standalone"] ??
		false) ||
	document.referrer.includes("android-app://");

export class ClientBusyError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "CreateClientError";
	}
}

export type ConnectionStatus =
	| "disconnected"
	| "connected"
	| "connecting"
	| "failed";

export type PeerbitLike = {
	peerId: PeerId;
	identity: Identity<PublicSignKey>;
	getMultiaddrs: () => Multiaddr[];
	dial: (address: string | Multiaddr | Multiaddr[]) => Promise<boolean>;
	hangUp: (
		address: PeerId | PublicSignKey | string | Multiaddr,
	) => Promise<void>;
	start: () => Promise<void>;
	stop: () => Promise<void>;
	bootstrap?: (addresses?: string[] | Multiaddr[]) => Promise<void>;
	open: <S extends Program<any>>(
		storeOrAddress: S | Address | string,
		options?: OpenOptions<S>,
	) => Promise<S>;
};

export type PeerRuntime = "node" | "canonical";

export type NodePeerProviderConfig = {
	runtime: "node";
	network: "local" | "remote" | NetworkOption;
	waitForConnected?: boolean | "in-flight";
	keypair?: Ed25519Keypair;
	singleton?: boolean;
	indexer?: (directory?: string) => Promise<Indices> | Indices;
	inMemory?: boolean;
};

export type CanonicalPeerProviderConfig = {
	runtime: "canonical";
	transport:
		| { kind: "service-worker"; options: ConnectServiceWorkerOptions }
		| {
				kind: "shared-worker";
				worker:
					| SharedWorker
					| (() => SharedWorker)
					| {
							url: string | URL;
							name?: string;
							type?: WorkerOptions["type"];
					  };
		  }
		| { kind: "window"; options?: ConnectWindowOptions }
		| {
				kind: "custom";
				connect: () => Promise<CanonicalClient>;
		  };
	open?: { adapters?: CanonicalOpenAdapter[]; mode?: CanonicalOpenMode };
	keepAlive?: false | Parameters<CanonicalClient["startKeepAlive"]>[0];
};

export type PeerProviderEnv = {
	inIframe: boolean;
};

export type PeerProviderConfig =
	| NodePeerProviderConfig
	| CanonicalPeerProviderConfig;

export type PeerProviderConfigSelector =
	| PeerProviderConfig
	| ((env: PeerProviderEnv) => PeerProviderConfig);

export type IPeerContext = {
	runtime: PeerRuntime;
	peer: PeerbitLike | undefined;
	promise: Promise<void> | undefined;
	loading: boolean;
	status: ConnectionStatus;
	persisted?: boolean;
	tabIndex?: number;
	error?: Error;
	canonical?: {
		client: CanonicalClient;
	};
};

if (!window.name) {
	window.name = uuid();
}

export const PeerContext = React.createContext<IPeerContext>({} as any);
export const usePeer = () => React.useContext(PeerContext);

/**
 * Network configuration for the node client.
 *
 * Prefer the bootstrap form when you want to dial explicit addresses.
 * If bootstrap is provided, it takes precedence over any implicit defaults.
 */
export type NetworkOption =
	| { type: "local" }
	| { type: "remote" }
	| { type?: "explicit"; bootstrap: (Multiaddr | string)[] };

const subscribeToUnload = (fn: () => any) => {
	window.addEventListener("pagehide", fn);
	window.addEventListener("beforeunload", fn);
	return () => {
		window.removeEventListener("pagehide", fn);
		window.removeEventListener("beforeunload", fn);
	};
};

export type PeerProviderProps = {
	config: PeerProviderConfigSelector;
	children: JSX.Element;
};

const resolveConfig = (
	input: PeerProviderConfigSelector,
): PeerProviderConfig =>
	typeof input === "function" ? input({ inIframe: inIframe() }) : input;

export const PeerProvider = ({ config, children }: PeerProviderProps) => {
	const [runtime, setRuntime] = React.useState<PeerRuntime>("node");
	const [peer, setPeer] = React.useState<PeerbitLike | undefined>(undefined);
	const [canonicalClient, setCanonicalClient] = React.useState<
		CanonicalClient | undefined
	>(undefined);
	const [promise, setPromise] = React.useState<Promise<void> | undefined>(
		undefined,
	);
	const [persisted, setPersisted] = React.useState<boolean | undefined>(
		undefined,
	);
	const [loading, setLoading] = React.useState<boolean>(true);
	const [connectionState, setConnectionState] =
		React.useState<ConnectionStatus>("disconnected");
	const [tabIndex, setTabIndex] = React.useState<number | undefined>(undefined);
	const [error, setError] = React.useState<Error | undefined>(undefined);

	const memo = React.useMemo<IPeerContext>(() => {
		return {
			runtime,
			peer,
			promise,
			loading,
			status: connectionState,
			persisted,
			tabIndex,
			error,
			canonical: canonicalClient ? { client: canonicalClient } : undefined,
		};
	}, [
		canonicalClient,
		connectionState,
		error,
		loading,
		peer,
		persisted,
		promise,
		runtime,
		tabIndex,
	]);

	React.useEffect(() => {
		let unmounted = false;
		let unloadUnsubscribe: (() => void) | undefined;
		let stopKeepAlive: (() => void) | undefined;
		let closePeer: (() => void) | undefined;

		const selected = resolveConfig(config);
		setRuntime(selected.runtime);
		setConnectionState("connecting");
		setLoading(true);
		setError(undefined);

		const fn = async () => {
			if (selected.runtime === "canonical") {
				const {
					connectServiceWorker,
					connectSharedWorker,
					connectWindow,
					PeerbitCanonicalClient,
				} = await import("@peerbit/canonical-client");

				let canonical: CanonicalClient;
				if (selected.transport.kind === "service-worker") {
					canonical = await connectServiceWorker(selected.transport.options);
				} else if (selected.transport.kind === "window") {
					canonical = await connectWindow(selected.transport.options ?? {});
				} else if (selected.transport.kind === "shared-worker") {
					const workerSpec = selected.transport.worker;
					const worker =
						typeof workerSpec === "function"
							? workerSpec()
							: workerSpec instanceof SharedWorker
								? workerSpec
								: new SharedWorker(workerSpec.url, {
										name: workerSpec.name,
										type: workerSpec.type,
									});
					canonical = await connectSharedWorker(worker);
				} else {
					canonical = await selected.transport.connect();
				}

				setCanonicalClient(canonical);
				stopKeepAlive =
					selected.keepAlive === false
						? undefined
						: canonical.startKeepAlive(selected.keepAlive);

				const peer = await PeerbitCanonicalClient.create(
					canonical,
					selected.open,
				);
				closePeer = () => {
					stopKeepAlive?.();
					try {
						peer.close();
					} catch {}
				};
				unloadUnsubscribe = subscribeToUnload(() => closePeer?.());
				if (unmounted) {
					closePeer();
					return;
				}
				setPeer(peer as unknown as PeerbitLike);
				setConnectionState("connected");
				setLoading(false);
				return;
			}

			const nodeOptions = selected as NodePeerProviderConfig;

			const [
				{ detectIncognito },
				sodiumModule,
				{ Peerbit },
				{ noise },
				{ yamux },
				{ webSockets },
			] = await Promise.all([
				import("detectincognitojs"),
				import("libsodium-wrappers"),
				import("peerbit"),
				import("@chainsafe/libp2p-noise"),
				import("@chainsafe/libp2p-yamux"),
				import("@libp2p/websockets"),
			]);

			const sodium = (sodiumModule as any).default ?? sodiumModule;
			await sodium.ready;

			let newPeer: PeerbitLike;
			let persistedResolved = false;
			const keepAliveRef = { current: true } as { current: boolean };

			const releaseFirstLock = cookiesWhereClearedJustNow();
			const sessionId = getClientId("session");
			const mutex = new FastMutex({ clientId: sessionId, timeout: 1e3 });

			if (nodeOptions.singleton) {
				singletonLog("acquiring lock");
				const localId = getClientId("local");
				try {
					const lockKey = localId + "-singleton";
					const unsubscribeUnload = subscribeToUnload(() => {
						keepAliveRef.current = false;
						mutex.release(lockKey);
					});
					const onVisibility = () => {
						if (document.visibilityState === "hidden") {
							keepAliveRef.current = false;
							try {
								mutex.release(lockKey);
							} catch {}
						}
					};
					document.addEventListener("visibilitychange", onVisibility);
					if (isInStandaloneMode()) {
						keepAliveRef.current = false;
						mutex.release(lockKey);
					}
					await mutex.lock(lockKey, () => keepAliveRef.current, {
						replaceIfSameClient: true,
					});
					singletonLog("lock acquired");
					void unsubscribeUnload;
				} catch (error) {
					console.error("Failed to lock singleton client", error);
					throw new ClientBusyError("Failed to lock single client");
				}
			}

			let nodeId: Ed25519Keypair;
			if (nodeOptions.keypair) {
				nodeId = nodeOptions.keypair;
			} else {
				keypairLog("acquiring lock");
				const kp = await getFreeKeypair("", mutex, () => keepAliveRef.current, {
					releaseFirstLock,
					releaseLockIfSameId: true,
				});
				keypairLog("lock acquired", { index: kp.index });
				subscribeToUnload(() => {
					keepAliveRef.current = false;
					mutex.release(kp.path);
				});
				nodeId = kp.key;
				setTabIndex(kp.index);
			}

			const peerId = nodeId.toPeerId();

			let directory: string | undefined;
			if (!nodeOptions.inMemory && !(await detectIncognito()).isPrivate) {
				storageLog("requesting persist");
				const persistedValue = await navigator.storage.persist();
				setPersisted(persistedValue);
				persistedResolved = persistedValue;
				if (!persistedValue) {
					console.error(
						"Request persistence but permission was not granted by browser.",
					);
				} else {
					directory = `./repo/${peerId.toString()}/`;
				}
			}

			clientLog("create", { directory });
			const created = await Peerbit.create({
				libp2p: {
					addresses: { listen: [] },
					streamMuxers: [yamux()],
					connectionEncrypters: [noise()],
					peerId,
					connectionManager: { maxConnections: 100 },
					connectionMonitor: { enabled: false },
					...(nodeOptions.network === "local"
						? {
								connectionGater: { denyDialMultiaddr: () => false },
								transports: [webSockets({})],
							}
						: {
								connectionGater: { denyDialMultiaddr: () => false },
								transports: [webSockets()],
							}),
				},
				directory,
				indexer: nodeOptions.indexer,
			});

			newPeer = created as unknown as PeerbitLike;

			(window as any).__peerInfo = {
				peerHash: created?.identity?.publicKey?.hashcode?.(),
				persisted: persistedResolved,
			};
			window.dispatchEvent(
				new CustomEvent("peer:ready", { detail: (window as any).__peerInfo }),
			);

			const connectFn = async () => {
				try {
					const network = nodeOptions.network;
					if (
						typeof network !== "string" &&
						(network as any)?.bootstrap !== undefined
					) {
						const list = (network as any).bootstrap as (Multiaddr | string)[];
						if (list.length === 0) {
							bootstrapLog("offline: skipping relay dialing");
						} else {
							for (const addr of list) {
								await created.dial(addr as any);
							}
						}
					} else if (
						network === "local" ||
						(typeof network !== "string" && (network as any)?.type === "local")
					) {
						const localAddress =
							"/ip4/127.0.0.1/tcp/8002/ws/p2p/" +
							(await (await fetch("http://localhost:8082/peer/id")).text());
						bootstrapLog("dialing local address", localAddress);
						await created.dial(localAddress);
					} else {
						await created.bootstrap?.();
					}
					setConnectionState("connected");
				} catch (err: any) {
					console.error("Failed to bootstrap:", err);
					setConnectionState("failed");
				}
			};

			const promise = connectFn();
			if (nodeOptions.waitForConnected === true) {
				await promise;
			} else if (nodeOptions.waitForConnected === "in-flight") {
				let isDone = false;
				promise.finally(() => {
					isDone = true;
				});
				await waitFor(() => {
					if (isDone) return true;
					const libp2p = created as any;
					if (libp2p.libp2p?.getDialQueue?.()?.length > 0) return true;
					if (libp2p.libp2p?.getConnections?.()?.length > 0) return true;
					return false;
				});
			}

			if (unmounted) {
				try {
					await (created as any)?.stop?.();
				} catch {}
				return;
			}
			setPeer(newPeer);
			setLoading(false);
		};

		const fnWithErrorHandling = async () => {
			try {
				await fn();
			} catch (error: any) {
				setError(error);
				setConnectionState("failed");
				setLoading(false);
			}
		};

		const p = fnWithErrorHandling();
		setPromise(p);
		return () => {
			unmounted = true;
			unloadUnsubscribe?.();
			closePeer?.();
		};
	}, [config]);

	return <PeerContext.Provider value={memo}>{children}</PeerContext.Provider>;
};
