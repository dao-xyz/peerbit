import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { webSockets } from "@libp2p/websockets";
import type { Multiaddr } from "@multiformats/multiaddr";
import { Ed25519Keypair } from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import { logger as createLogger } from "@peerbit/logger";
import type { ProgramClient } from "@peerbit/program";
import { createClient, createHost } from "@peerbit/proxy-window";
import { waitFor } from "@peerbit/time";
import { detectIncognito } from "detectincognitojs";
import sodium from "libsodium-wrappers";
import { Peerbit } from "peerbit";
import * as React from "react";
import type { JSX } from "react";
import { v4 as uuid } from "uuid";
import { FastMutex } from "./lockstorage.ts";
import { useMount } from "./useMount.ts";
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

/** Discriminated union for PeerContext */
export type IPeerContext = (ProxyPeerContext | NodePeerContext) & {
	error?: Error;
};

export interface ProxyPeerContext {
	type: "proxy";
	peer: ProgramClient | undefined;
	promise: Promise<void> | undefined;
	loading: boolean;
	status: ConnectionStatus;
	persisted: boolean | undefined;
	/** Present only in proxy (iframe) mode */
	targetOrigin: string;
}

export interface NodePeerContext {
	type: "node";
	peer: ProgramClient | undefined;
	promise: Promise<void> | undefined;
	loading: boolean;
	status: ConnectionStatus;
	persisted: boolean | undefined;
	tabIndex: number;
}

if (!window.name) {
	window.name = uuid();
}

export const PeerContext = React.createContext<IPeerContext>({} as any);
export const usePeer = () => React.useContext(PeerContext);

type IFrameOptions = {
	type: "proxy";
	targetOrigin: string;
};

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

type NodeOptions = {
	type?: "node";
	network: "local" | "remote" | NetworkOption;
	waitForConnnected?: boolean | "in-flight";
	keypair?: Ed25519Keypair;
	host?: boolean;
	singleton?: boolean;
	indexer?: (directory?: string) => Promise<Indices> | Indices;
};

type TopOptions = NodeOptions & { inMemory?: boolean };
type TopAndIframeOptions = {
	iframe: IFrameOptions | NodeOptions;
	top: TopOptions;
};
type WithChildren = {
	children: JSX.Element;
};
type PeerOptions = (TopAndIframeOptions | TopOptions) & WithChildren;

const subscribeToUnload = (fn: () => any) => {
	window.addEventListener("pagehide", fn);
	window.addEventListener("beforeunload", fn);
};

export const PeerProvider = (options: PeerOptions) => {
	const [peer, setPeer] = React.useState<ProgramClient | undefined>(undefined);
	const [promise, setPromise] = React.useState<Promise<void> | undefined>(
		undefined,
	);
	const [persisted, setPersisted] = React.useState<boolean>(false);
	const [loading, setLoading] = React.useState<boolean>(true);
	const [connectionState, setConnectionState] =
		React.useState<ConnectionStatus>("disconnected");

	const [tabIndex, setTabIndex] = React.useState<number>(-1);

	const [error, setError] = React.useState<Error | undefined>(undefined); // <-- error state

	// Decide which options to use based on whether we're in an iframe.
	// If options.top is defined, assume we have separate settings for iframe vs. host.
	const nodeOptions: IFrameOptions | TopOptions = (
		options as TopAndIframeOptions
	).top
		? inIframe()
			? (options as TopAndIframeOptions).iframe
			: { ...options, ...(options as TopAndIframeOptions).top } // we merge root and top options, TODO should this be made in a different way to prevent confusion about top props?
		: (options as TopOptions);

	// If running as a proxy (iframe), expect a targetOrigin.
	const computedTargetOrigin =
		nodeOptions.type === "proxy"
			? (nodeOptions as IFrameOptions).targetOrigin
			: undefined;

	const memo = React.useMemo<IPeerContext>(() => {
		if (nodeOptions.type === "proxy") {
			return {
				type: "proxy",
				peer,
				promise,
				loading,
				status: connectionState,
				persisted,
				targetOrigin: computedTargetOrigin as string,
				error,
			};
		} else {
			return {
				type: "node",
				peer,
				promise,
				loading,
				status: connectionState,
				persisted,
				tabIndex,
				error,
			};
		}
	}, [
		loading,
		promise,
		connectionState,
		peer,
		persisted,
		tabIndex,
		computedTargetOrigin,
		error,
	]);

	useMount(() => {
		setLoading(true);
		const fn = async () => {
			await sodium.ready;
			let newPeer: ProgramClient;
			// Track resolved persistence status during client creation
			let persistedResolved = false;
			// Controls how long we keep locks alive; flipped to false on close/hidden
			const keepAliveRef = { current: true } as { current: boolean };

			if (nodeOptions.type !== "proxy") {
				const releaseFirstLock = cookiesWhereClearedJustNow();

				const sessionId = getClientId("session");
				const mutex = new FastMutex({
					clientId: sessionId,
					timeout: 1e3,
				});
				if (nodeOptions.singleton) {
					singletonLog("acquiring lock");
					const localId = getClientId("local");
					try {
						const lockKey = localId + "-singleton";
						subscribeToUnload(function () {
							// Immediate release on page close
							keepAliveRef.current = false;
							mutex.release(lockKey);
						});
						// Also release when page is hidden to reduce flakiness between sequential tests
						const onVisibility = () => {
							if (document.visibilityState === "hidden") {
								keepAliveRef.current = false;
								// Mark expired and remove proactively
								try {
									mutex.release(lockKey);
								} catch {}
							}
						};
						document.addEventListener("visibilitychange", onVisibility);
						if (isInStandaloneMode()) {
							// PWA issue fix (? TODO is this needed ?
							keepAliveRef.current = false;
							mutex.release(lockKey);
						}
						await mutex.lock(lockKey, () => keepAliveRef.current, {
							replaceIfSameClient: true,
						});
						singletonLog("lock acquired");
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
					const kp = await getFreeKeypair(
						"",
						mutex,
						() => keepAliveRef.current,
						{
							releaseFirstLock,
							releaseLockIfSameId: true,
						},
					);
					keypairLog("lock acquired", { index: kp.index });
					subscribeToUnload(function () {
						keepAliveRef.current = false;
						mutex.release(kp.path);
					});
					nodeId = kp.key;
					setTabIndex(kp.index);
				}
				const peerId = nodeId.toPeerId();

				let directory: string | undefined = undefined;
				if (
					!(nodeOptions as TopOptions).inMemory &&
					!(await detectIncognito()).isPrivate
				) {
					storageLog("requesting persist");
					const persisted = await navigator.storage.persist();
					setPersisted(persisted);
					persistedResolved = persisted;
					if (!persisted) {
						setPersisted(false);
						console.error(
							"Request persistence but permission was not granted by browser.",
						);
					} else {
						directory = `./repo/${peerId.toString()}/`;
					}
				}

				clientLog("create", { directory });
				newPeer = await Peerbit.create({
					libp2p: {
						addresses: {
							listen: [
								/* "/p2p-circuit" */
							],
						},
						streamMuxers: [yamux()],
						connectionEncrypters: [noise()],
						peerId,
						connectionManager: { maxConnections: 100 },
						connectionMonitor: { enabled: false },
						...(nodeOptions.network === "local"
							? {
									connectionGater: {
										denyDialMultiaddr: () => false,
									},
									transports: [
										webSockets({}) /* ,
                                    circuitRelayTransport(), */,
									],
								}
							: {
									connectionGater: {
										denyDialMultiaddr: () => false, // TODO do right here, dont allow local dials except bootstrap
									},
									transports: [
										webSockets() /* ,
                                    circuitRelayTransport(), */,
									],
								}) /* 
                        services: {
                            pubsub: (c) =>
                                new DirectSub(c, { canRelayMessage: true }),
                            identify: identify(),
                        }, */,
					},
					directory,
					indexer: nodeOptions.indexer,
				});
				clientLog("created", {
					directory,
					peerHash: newPeer?.identity.publicKey.hashcode(),
					network: nodeOptions.network === "local" ? "local" : "remote",
				});

				(window as any).__peerInfo = {
					peerHash: newPeer?.identity.publicKey.hashcode(),
					persisted: persistedResolved,
				};
				window.dispatchEvent(
					new CustomEvent("peer:ready", {
						detail: (window as any).__peerInfo,
					}),
				);

				setConnectionState("connecting");

				const connectFn = async () => {
					try {
						const network = nodeOptions.network;

						// 1) Explicit bootstrap addresses take precedence
						if (
							typeof network !== "string" &&
							(network as any)?.bootstrap !== undefined
						) {
							const list = (network as any).bootstrap as (Multiaddr | string)[];
							if (list.length === 0) {
								// Explicit offline mode: skip dialing and mark as connected (no relays)
								bootstrapLog("offline: skipping relay dialing");
							} else {
								for (const addr of list) {
									await newPeer.dial(addr);
								}
							}
						}
						// 2) Local development: dial local relay service
						else if (
							network === "local" ||
							(typeof network !== "string" &&
								(network as any)?.type === "local")
						) {
							const localAddress =
								"/ip4/127.0.0.1/tcp/8002/ws/p2p/" +
								(await (await fetch("http://localhost:8082/peer/id")).text());
							bootstrapLog("dialing local address", localAddress);
							await newPeer.dial(localAddress);
						}
						// 3) Remote default: use bootstrap service (no explicit bootstrap provided)
						else {
							await (newPeer as Peerbit).bootstrap?.();
						}
						setConnectionState("connected");
					} catch (err: any) {
						console.error("Failed to bootstrap:", err);
						setConnectionState("failed");
					}

					if (nodeOptions.host) {
						newPeer = await createHost(newPeer as Peerbit);
					}
				};

				const perfEnabled = new URLSearchParams(window.location.search).get(
					"perf",
				);
				const t0 = performance.now();
				const marks: Record<string, number> = {};
				const perfMark = (label: string) => {
					marks[label] = performance.now() - t0;
				};

				bootstrapLog("start...");
				const promise = connectFn().then(() => {
					perfMark("dialComplete");
				});
				promise.then(() => {
					bootstrapLog("done");
					try {
						if (perfEnabled) {
							const payload = { ...marks } as any;
							console.info("[Perf] peer bootstrap", payload);
							window.dispatchEvent(
								new CustomEvent("perf:peer", {
									detail: payload,
								}),
							);
						}
					} catch {}
				});
				if (nodeOptions.waitForConnnected === true) {
					await promise;
				} else if (nodeOptions.waitForConnnected === "in-flight") {
					// wait for dialQueue to not be empty or connections to contains the peerId
					// or done
					let isDone = false;
					promise.finally(() => {
						isDone = true;
					});
					await waitFor(() => {
						if (isDone) {
							return true;
						}
						const libp2p = newPeer as Peerbit;
						if (libp2p.libp2p.getDialQueue().length > 0) {
							return true;
						}
						if (libp2p.libp2p.getConnections().length > 0) {
							return true;
						}
						return false;
					});
				}
			} else {
				// When in proxy mode (iframe), use the provided targetOrigin.
				newPeer = await createClient(
					(nodeOptions as IFrameOptions).targetOrigin,
				);
				try {
					(window as any).__peerInfo = {
						peerHash: newPeer?.identity.publicKey.hashcode(),
						persisted: false,
					};
					window.dispatchEvent(
						new CustomEvent("peer:ready", {
							detail: (window as any).__peerInfo,
						}),
					);
				} catch {}
			}

			setPeer(newPeer);
			setLoading(false);
		};
		const fnWithErrorHandling = async () => {
			try {
				await fn();
			} catch (error: any) {
				setError(error);
				setLoading(false);
			}
		};
		setPromise(fnWithErrorHandling());
	});

	return (
		<PeerContext.Provider value={memo}>{options.children}</PeerContext.Provider>
	);
};
