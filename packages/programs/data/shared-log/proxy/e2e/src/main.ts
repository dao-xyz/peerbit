import {
	type CanonicalChannel,
	CanonicalClient,
	PeerbitCanonicalClient,
	connectServiceWorker,
	connectSharedWorker,
} from "@peerbit/canonical-client";
import { connectWindow } from "@peerbit/canonical-client/window";
import { installWindowHost } from "@peerbit/canonical-host/window";
import { SharedLog } from "@peerbit/shared-log";
import { sharedLogAdapter } from "@peerbit/shared-log-proxy/auto";
import type { SharedLogProxy } from "@peerbit/shared-log-proxy/client";
import { makeTestId } from "./types.js";
import { createCanonicalHost } from "./worker-app.js";

const statusEl = document.querySelector(
	'[data-testid="status"]',
) as HTMLElement;
const peerIdEl = document.querySelector(
	'[data-testid="peer-id"]',
) as HTMLElement;
const entryCountEl = document.querySelector(
	'[data-testid="entry-count"]',
) as HTMLElement;
const entriesEl = document.querySelector(
	'[data-testid="entries"]',
) as HTMLUListElement;

const params = new URLSearchParams(window.location.search);
const labelParam = params.get("label");
const label =
	labelParam && labelParam.length > 0
		? labelParam
		: `edge-${Math.random().toString(36).slice(2, 8)}`;
const scenario = params.get("scenario") || "shared-log";
const session = params.get("session") || "default";
const transport = params.get("transport") || "sharedworker";
const autoClose = params.get("autoClose") !== "false";
const role = params.get("role") || "child";
const channel = params.get("channel") || `peerbit-window-${session}`;
const keepAliveIntervalMs = Number(params.get("keepAliveIntervalMs") ?? "2000");
const keepAliveTimeoutMs = Number(params.get("keepAliveTimeoutMs") ?? "10000");
const keepAliveFailures = Number(params.get("keepAliveFailures") ?? "2");

const requestedAddress = params.get("address") || undefined;

const ids = {
	sharedLog: makeTestId(`shared-log:${session}`, 1),
};

const toHex = (bytes: Uint8Array): string => {
	let out = "";
	for (const b of bytes) out += b.toString(16).padStart(2, "0");
	return out;
};

let renderedEntries: string[] = [];
const renderEntries = () => {
	entryCountEl.textContent = String(renderedEntries.length);
	entriesEl.innerHTML = "";
	for (const item of renderedEntries) {
		const li = document.createElement("li");
		li.textContent = item;
		entriesEl.appendChild(li);
	}
};

type DebugStats = {
	sharedLogs?: {
		total: number;
		entries?: Array<{ key: string; refs: number }>;
	};
};

type DebugAppendResponse = { id: number; ok: true } | { id: number; ok: false };

const createDebugClient = (client: CanonicalClient) => {
	let portPromise: Promise<CanonicalChannel> | undefined;
	let port: CanonicalChannel | undefined;
	let nextId = 1;
	const pending = new Map<
		number,
		{
			resolve: (value: any) => void;
			reject: (error: Error) => void;
			timeout?: ReturnType<typeof setTimeout>;
		}
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
			if (entry.timeout) clearTimeout(entry.timeout);
			if (message.ok === false) {
				entry.reject(new Error(message.error ?? "Unknown debug error"));
			} else {
				entry.resolve(message.stats ?? message);
			}
		});
		return port;
	};

	const request = async <T>(body: object, timeoutMs = 5_000): Promise<T> => {
		const channel = await ensurePort();
		const id = nextId++;
		const payload = encoder.encode(JSON.stringify({ id, ...body }));
		return new Promise<T>((resolve, reject) => {
			const timeout = setTimeout(() => {
				if (!pending.has(id)) return;
				pending.delete(id);
				reject(
					new Error(`Debug request timeout (${String((body as any).op)})`),
				);
			}, timeoutMs);
			pending.set(id, { resolve, reject, timeout });
			channel.send(payload);
		});
	};

	return {
		getStats: async () => request<DebugStats>({ op: "stats" }),
		append: async (logId: Uint8Array, text: string) => {
			await request<DebugAppendResponse>({
				op: "append",
				logId: toHex(logId),
				text,
			});
		},
		emitEvent: async (logId: Uint8Array, type: string) => {
			await request<DebugAppendResponse>({
				op: "emitEvent",
				logId: toHex(logId),
				type,
			});
		},
		saveProgram: async (logId: Uint8Array) => {
			const resp = await request<{ address: string }>({
				op: "saveProgram",
				logId: toHex(logId),
			});
			return resp.address;
		},
	};
};

const connectSharedWorkerClient = async (): Promise<{
	client: CanonicalClient;
}> => {
	const worker = new SharedWorker(new URL("./worker.ts", import.meta.url), {
		name: "peerbit-canonical-shared-log",
		type: "module",
	});
	const pingId = 0;
	const onPing = (event: MessageEvent) => {
		if (event.data?.id !== pingId) return;
		worker.port.removeEventListener("message", onPing);
		statusEl.textContent = "connecting (worker)";
	};
	worker.port.addEventListener("message", onPing);
	worker.port.start();
	try {
		worker.port.postMessage({ id: pingId, op: "__ping__" });
	} catch (error) {
		console.error("shared worker ping failed", error);
	}
	worker.onerror = (event) => {
		console.error("shared worker error", event);
		const message =
			(event as any)?.message ||
			(event as any)?.error?.message ||
			String(event);
		statusEl.textContent = `error: ${message}`;
	};

	const client = await connectSharedWorker(worker);
	return { client };
};

const init = async () => {
	statusEl.textContent = "connecting";
	let closeFns: Array<() => Promise<void>> = [];
	let api: any;
	let client: CanonicalClient | undefined;

	if (transport === "window" && role === "parent") {
		statusEl.textContent = "hosting (window)";
		const host = await createCanonicalHost();
		const windowHost = installWindowHost({
			host,
			channel,
			targetOrigin: "*",
			onError: (error) => {
				console.error("window host error", error);
			},
		});
		closeFns.push(async () => {
			windowHost.close();
		});

		const hostPeerId = await host.ctx.peerId();
		peerIdEl.textContent = hostPeerId;
		api = {
			scenario: "host",
			peerId: hostPeerId,
			close: async () => {
				for (const fn of closeFns) {
					await fn();
				}
			},
		};

		(window as any).__canonicalTest = api;
		statusEl.textContent = "ready";
		renderEntries();
		return;
	}

	if (transport === "serviceworker") {
		statusEl.textContent = "connecting (service worker)";
		client = await connectServiceWorker({
			url: new URL("../service-worker.ts", import.meta.url),
			scope: "/",
			type: "module",
			timeoutMs: 10_000,
		});
	} else if (transport === "window") {
		statusEl.textContent = "connecting (window)";
		client = await connectWindow({
			channel,
			targetOrigin: "*",
			timeoutMs: 10_000,
		});
	} else {
		statusEl.textContent = "connecting (worker)";
		const connected = await connectSharedWorkerClient();
		client = connected.client;
	}

	if (!client) {
		throw new Error("Canonical client not initialized");
	}

	const stopKeepAlive = client.startKeepAlive({
		intervalMs: keepAliveIntervalMs,
		timeoutMs: keepAliveTimeoutMs,
		maxFailures: keepAliveFailures,
	});
	closeFns.push(async () => {
		stopKeepAlive();
	});

	const debug = createDebugClient(client);
	const canonicalPeerId = await client.peerId();
	peerIdEl.textContent = canonicalPeerId;

	let sharedLog: SharedLogProxy | undefined;
	const decoder = new TextDecoder();
	const refresh = async () => {
		if (!sharedLog) return [];
		const items = await sharedLog.log.toArray();
		const texts: string[] = [];
		for (const entry of items) {
			const value = await entry.getPayloadValue();
			if (value instanceof Uint8Array) {
				texts.push(decoder.decode(value));
			} else {
				texts.push(String(value));
			}
		}
		renderedEntries = texts;
		renderEntries();
		return texts;
	};

	const waitForEvent = async (
		type: string,
		timeoutMs = 5_000,
	): Promise<{ type: string; publicKeyHash?: string }> => {
		if (!sharedLog) {
			throw new Error("Shared log not open");
		}
		return new Promise((resolve, reject) => {
			let timeout: ReturnType<typeof setTimeout> | undefined;
			const handler = (event: any) => {
				if (timeout) clearTimeout(timeout);
				sharedLog!.events.removeEventListener(type, handler as any);
				const detail = event?.detail;
				const key =
					detail && typeof detail === "object" && "publicKey" in detail
						? (detail as any).publicKey
						: detail;
				const publicKeyHash =
					key && typeof key.hashcode === "function"
						? key.hashcode()
						: undefined;
				resolve({ type, publicKeyHash });
			};

			timeout = setTimeout(() => {
				sharedLog!.events.removeEventListener(type, handler as any);
				reject(new Error(`Timed out waiting for event '${type}'`));
			}, timeoutMs);

			sharedLog!.events.addEventListener(type, handler as any);
		});
	};

	if (scenario === "shared-log") {
		const peer = await PeerbitCanonicalClient.create(client, {
			adapters: [sharedLogAdapter],
		});

		if (requestedAddress) {
			sharedLog = (await peer.open(requestedAddress as any)) as any;
		} else {
			sharedLog = (await peer.open(
				new SharedLog<Uint8Array>({ id: ids.sharedLog }),
			)) as any;
		}

		await refresh();
		const interval = window.setInterval(() => {
			void refresh();
		}, 250);
		closeFns.push(async () => {
			clearInterval(interval);
		});

		closeFns.push(async () => {
			if (sharedLog) await sharedLog.close();
		});

		api = {
			scenario,
			peerId: canonicalPeerId,
			append: async (text: string) => {
				await debug.append(ids.sharedLog, `${label}: ${text}`);
				await refresh();
			},
			emitHostEvent: async (type: string) => {
				await debug.emitEvent(ids.sharedLog, type);
			},
			waitForEvent,
			refresh,
			getEntries: () => renderedEntries.slice(),
			saveAddress: async () => {
				return debug.saveProgram(ids.sharedLog);
			},
			getHostStats: async () => debug.getStats(),
			close: async () => {
				for (const fn of closeFns) {
					await fn();
				}
			},
		};
	} else if (scenario === "stats") {
		api = {
			scenario,
			peerId: canonicalPeerId,
			getHostStats: async () => debug.getStats(),
			close: async () => {
				for (const fn of closeFns) {
					await fn();
				}
			},
		};
	} else {
		throw new Error(`Unknown scenario '${scenario}'`);
	}

	closeFns.push(async () => {
		client.close();
	});

	(window as any).__canonicalTest = api;
	statusEl.textContent = "ready";
	renderEntries();

	if (autoClose) {
		window.addEventListener("beforeunload", () => {
			void api.close();
		});
	}
};

void init().catch((error) => {
	console.error("canonical e2e init failed", error);
	statusEl.textContent = `error: ${error?.message || String(error)}`;
});
