import {
	type CanonicalChannel,
	CanonicalClient,
	PeerbitCanonicalClient,
	connectServiceWorker,
	connectSharedWorker,
} from "@peerbit/canonical-client";
import { connectWindow } from "@peerbit/canonical-client/window";
import { installWindowHost } from "@peerbit/canonical-host/window";
import { Documents } from "@peerbit/document";
import { documentAdapter } from "@peerbit/document-proxy/auto";
import { useQuery } from "@peerbit/document-react";
import { SortDirection } from "@peerbit/indexer-interface";
import { useEffect, useMemo, useRef, useState } from "react";
import { TestMessage, makeTestId } from "./types.js";
import { createCanonicalHost } from "./worker-app.js";

const params = new URLSearchParams(window.location.search);
const labelParam = params.get("label");
const label =
	labelParam && labelParam.length > 0
		? labelParam
		: `peer-${Math.random().toString(36).slice(2, 8)}`;
const session = params.get("session") || "default";
const transport = params.get("transport") || "sharedworker";
const role = params.get("role") || "child";
const channel = params.get("channel") || `peerbit-window-${session}`;
const autoClose = params.get("autoClose") !== "false";
const keepAliveIntervalMs = Number(params.get("keepAliveIntervalMs") ?? "2000");
const keepAliveTimeoutMs = Number(params.get("keepAliveTimeoutMs") ?? "10000");
const keepAliveFailures = Number(params.get("keepAliveFailures") ?? "2");

type DebugStats = {
	documents?: { total: number; entries?: Array<{ key: string; refs: number }> };
};

const createDebugClient = (client: CanonicalClient) => {
	let portPromise: Promise<CanonicalChannel> | undefined;
	let port: CanonicalChannel | undefined;
	let nextId = 1;
	const pending = new Map<
		number,
		{ resolve: (value: any) => void; reject: (error: Error) => void }
	>();
	const encoder = new TextEncoder();
	const decoder = new TextDecoder();

	const ensurePort = async () => {
		if (port) return port;
		if (!portPromise) {
			portPromise = client.openPort("@peerbit/debug", new Uint8Array());
		}
		port = await portPromise;
		port.onMessage((payload) => {
			let message: any;
			try {
				message = JSON.parse(decoder.decode(payload));
			} catch {
				return;
			}
			const entry = pending.get(message?.id);
			if (!entry) return;
			pending.delete(message.id);
			if (message.ok === false) {
				entry.reject(new Error(message.error ?? "Unknown debug error"));
			} else {
				entry.resolve(message.stats ?? message);
			}
		});
		return port;
	};

	const request = async <T,>(op: string): Promise<T> => {
		const channel = await ensurePort();
		const id = nextId++;
		const payload = encoder.encode(JSON.stringify({ id, op }));
		return new Promise<T>((resolve, reject) => {
			pending.set(id, { resolve, reject });
			channel.send(payload);
		});
	};

	return {
		getStats: async () => request<DebugStats>("stats"),
	};
};

const makeSharedWorkerClient = async (): Promise<CanonicalClient> => {
	const worker = new SharedWorker(new URL("./worker.ts", import.meta.url), {
		name: "peerbit-canonical-documents-react",
		type: "module",
	});
	return connectSharedWorker(worker);
};

type AppState = {
	status: string;
	peerId?: string;
	documents?: Documents<TestMessage> | any;
	error?: string;
};

export const App = () => {
	const [state, setState] = useState<AppState>({ status: "loading" });
	const [message, setMessage] = useState("");
	const closeRef = useRef<() => Promise<void>>();

	useEffect(() => {
		let stopped = false;
		const closeFns: Array<() => Promise<void> | void> = [];

		const close = async () => {
			for (const fn of closeFns.splice(0)) {
				try {
					await fn();
				} catch {
					// ignore close errors
				}
			}
		};
		closeRef.current = close;

		const init = async () => {
			if (transport === "window" && role === "parent") {
				setState({ status: "hosting (window)" });
				const host = createCanonicalHost();
				const windowHost = installWindowHost({
					host,
					channel,
					targetOrigin: "*",
					onError: (error) => {
						console.error("window host error", error);
					},
				});
				closeFns.push(async () => windowHost.close());

				const hostPeerId = await host.ctx.peerId();
				if (stopped) return;
				setState({ status: "ready", peerId: hostPeerId });
				(window as any).__canonicalTest = {
					role: "host",
					peerId: hostPeerId,
					close,
				};
				return;
			}

			setState({ status: "connecting" });
			let client: CanonicalClient;

			if (transport === "serviceworker") {
				client = await connectServiceWorker({
					url: new URL("../service-worker.ts", import.meta.url),
					scope: "/",
					type: "module",
					timeoutMs: 10_000,
				});
			} else if (transport === "window") {
				client = await connectWindow({
					channel,
					targetOrigin: "*",
					timeoutMs: 10_000,
				});
			} else {
				client = await makeSharedWorkerClient();
			}

			if (stopped) {
				client.close();
				return;
			}

			const stopKeepAlive = client.startKeepAlive({
				intervalMs: keepAliveIntervalMs,
				timeoutMs: keepAliveTimeoutMs,
				maxFailures: keepAliveFailures,
			});
			closeFns.push(() => {
				stopKeepAlive();
			});

			const debug = createDebugClient(client);
			const canonicalPeerId = await client.peerId();
			if (stopped) {
				client.close();
				return;
			}

			const peer = await PeerbitCanonicalClient.create(client, {
				adapters: [documentAdapter],
			});
			const id = makeTestId(`documents-react:${session}`, 1);
			const docs = await peer.open(new Documents<TestMessage>({ id }), {
				args: { type: TestMessage },
			});

			closeFns.push(async () => {
				try {
					await docs.close();
				} catch {
					// ignore close errors
				}
			});
			closeFns.push(async () => {
				try {
					client.close();
				} catch {
					// ignore close errors
				}
			});

			if (stopped) {
				await close();
				return;
			}

			(window as any).__canonicalTest = {
				role: "client",
				peerId: canonicalPeerId,
				put: async (text: string) => {
					const now = BigInt(Date.now());
					const id = `${label}-${String(now)}-${Math.random()
						.toString(36)
						.slice(2, 8)}`;
					await docs.put(
						new TestMessage({ id, author: label, text, timestamp: now }),
					);
				},
				getHostStats: async () => debug.getStats(),
				close,
			};

			setState({ status: "ready", peerId: canonicalPeerId, documents: docs });

			const writeParam = params.get("write");
			if (writeParam && writeParam.length > 0) {
				const key = `__canonical_written_documents_react_${session}_${writeParam}`;
				if (!(window as any)[key]) {
					(window as any)[key] = true;
					for (const text of writeParam.split(",").map((s) => s.trim())) {
						if (!text) continue;
						await (window as any).__canonicalTest.put(text);
					}
				}
			}

			if (autoClose) {
				const onBeforeUnload = () => {
					void close();
				};
				window.addEventListener("beforeunload", onBeforeUnload);
				closeFns.push(() => {
					window.removeEventListener("beforeunload", onBeforeUnload);
				});
			}
		};

		void init().catch((error) => {
			console.error("canonical react e2e init failed", error);
			setState({
				status: "error",
				error: error instanceof Error ? error.message : String(error),
			});
		});

		return () => {
			stopped = true;
			void close();
		};
	}, []);

	const queryOptions = useMemo(
		() => ({
			query: {
				sort: [{ key: ["timestamp"], direction: SortDirection.ASC }],
			},
			resolve: true as const,
			prefetch: true,
			local: true,
			remote: false as const,
			updates: { merge: true, push: true },
		}),
		[],
	);

	const query = useQuery<TestMessage, TestMessage, true>(state.documents, {
		...queryOptions,
	});

	const items = query.items ?? [];

	return (
		<main style={{ fontFamily: "sans-serif", padding: 16 }}>
			<h1>Document React Canonical E2E</h1>
			<p data-testid="status">{state.status}</p>
			<p data-testid="peer-id">{state.peerId ?? ""}</p>
			<p data-testid="message-count">{items.length}</p>
			<ul data-testid="messages">
				{items.map((item) => (
					<li
						key={item.id}
						data-id={item.id}
						data-timestamp={String(item.timestamp)}
						data-author={item.author}
					>
						{item.author}: {item.text}
					</li>
				))}
			</ul>

			<div style={{ display: "flex", gap: 8 }}>
				<input
					data-testid="message-input"
					type="text"
					value={message}
					onChange={(e) => setMessage(e.target.value)}
				/>
				<button
					data-testid="send-button"
					disabled={!state.documents || !message}
					onClick={async () => {
						const api = (window as any).__canonicalTest;
						if (!api?.put) {
							throw new Error("Canonical test API not ready");
						}
						await api.put(message);
						setMessage("");
					}}
				>
					Send
				</button>
				<button
					data-testid="close-button"
					disabled={!closeRef.current}
					onClick={async () => closeRef.current?.()}
				>
					Close
				</button>
			</div>

			{state.error ? (
				<pre data-testid="error" style={{ color: "crimson" }}>
					{state.error}
				</pre>
			) : null}
		</main>
	);
};
