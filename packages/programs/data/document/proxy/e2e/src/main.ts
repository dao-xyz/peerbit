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
import { counterAdapter } from "./counter-adapter.js";
import { CounterProgram } from "./counter-program.js";
import { MixedProgram } from "./mixed-program.js";
import { TestMessage, makeTestId } from "./types.js";
import { createCanonicalHost } from "./worker-app.js";

const statusEl = document.querySelector(
	'[data-testid="status"]',
) as HTMLElement;
const peerIdEl = document.querySelector(
	'[data-testid="peer-id"]',
) as HTMLElement;
const messageCountEl = document.querySelector(
	'[data-testid="message-count"]',
) as HTMLElement;
const messagesEl = document.querySelector(
	'[data-testid="messages"]',
) as HTMLUListElement;

const params = new URLSearchParams(window.location.search);
const labelParam = params.get("label");
const label =
	labelParam && labelParam.length > 0
		? labelParam
		: `edge-${Math.random().toString(36).slice(2, 8)}`;
const scenario = params.get("scenario") || "documents";
const session = params.get("session") || "default";
const transport = params.get("transport") || "sharedworker";
const autoClose = params.get("autoClose") !== "false";
const role = params.get("role") || "child";
const channel = params.get("channel") || `peerbit-window-${session}`;
const keepAliveIntervalMs = Number(params.get("keepAliveIntervalMs") ?? "2000");
const keepAliveTimeoutMs = Number(params.get("keepAliveTimeoutMs") ?? "10000");
const keepAliveFailures = Number(params.get("keepAliveFailures") ?? "2");

const ids = {
	documents: makeTestId(`documents:${session}`, 1),
	counter: makeTestId(`counter:${session}`, 3),
};

const messages = new Map<string, TestMessage>();
let changesListener: ((event: any) => void) | undefined;
let lastMessageId: string | undefined;

const renderMessages = () => {
	const items = [...messages.values()].sort((a, b) =>
		a.timestamp === b.timestamp ? 0 : a.timestamp < b.timestamp ? -1 : 1,
	);
	messageCountEl.textContent = String(items.length);
	messagesEl.innerHTML = "";
	for (const item of items) {
		const li = document.createElement("li");
		li.textContent = `${item.author}: ${item.text}`;
		li.setAttribute("data-id", item.id);
		li.setAttribute("data-timestamp", String(item.timestamp));
		messagesEl.appendChild(li);
	}
};

const buildMessage = (text: string): TestMessage => {
	const now = BigInt(Date.now());
	const id = `${label}-${String(now)}-${Math.random().toString(36).slice(2, 8)}`;
	return new TestMessage({
		id,
		author: label,
		text,
		timestamp: now,
	});
};

const attachDocuments = (docs: {
	changes: EventTarget;
	put: (doc: TestMessage) => Promise<void>;
	close: () => Promise<void>;
}) => {
	changesListener = (event: any) => {
		const detail = event.detail as {
			added: TestMessage[];
			removed: TestMessage[];
		};
		for (const added of detail.added ?? []) {
			messages.set(added.id, added);
		}
		for (const removed of detail.removed ?? []) {
			messages.delete(removed.id);
		}
		renderMessages();
	};
	docs.changes.addEventListener("change", changesListener);
	return async () => {
		if (changesListener) {
			docs.changes.removeEventListener("change", changesListener);
		}
		await docs.close();
	};
};

type DebugStats = {
	documents?: { total: number; entries?: Array<{ key: string; refs: number }> };
	counters?: { total: number; entries?: Array<{ key: string; refs: number }> };
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

	const request = async <T>(op: string): Promise<T> => {
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

const connectSharedWorkerClient = async (): Promise<{
	client: CanonicalClient;
}> => {
	const worker = new SharedWorker(new URL("./worker.ts", import.meta.url), {
		name: "peerbit-canonical-documents",
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
		renderMessages();
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

	if (scenario === "documents") {
		const peer = await PeerbitCanonicalClient.create(client, {
			adapters: [documentAdapter],
		});
		const docs = await peer.open(
			new Documents<TestMessage>({ id: ids.documents }),
			{
				args: { type: TestMessage },
			},
		);
		const detach = attachDocuments(docs as any);
		closeFns.push(detach);
		api = {
			scenario,
			put: async (text: string) => {
				const msg = buildMessage(text);
				lastMessageId = msg.id;
				await docs.put(msg);
			},
			getLastLogEntryHash: async () => {
				if (!lastMessageId) return { head: undefined, entryHash: undefined };
				const value = await docs.get(lastMessageId);
				const head = (value as any)?.__context?.head;
				const entry = head ? await (docs as any).log.log.get(head) : undefined;
				return { head, entryHash: entry?.hash };
			},
			getMessages: () => [...messages.values()],
			peerId: canonicalPeerId,
			close: async () => {
				for (const fn of closeFns) {
					await fn();
				}
			},
		};
	} else if (scenario === "mixed") {
		const peer = await PeerbitCanonicalClient.create(client, {
			adapters: [documentAdapter],
		});
		const mixed = new MixedProgram({ id: ids.documents });
		(mixed as any).node = peer as any;
		await mixed.open({ type: TestMessage });

		const docs = (mixed as any).docs;
		const detach = attachDocuments(docs as any);
		closeFns.push(detach);
		api = {
			scenario,
			put: async (text: string) => {
				const msg = buildMessage(text);
				lastMessageId = msg.id;
				await mixed.put(msg);
			},
			getLastLogEntryHash: async () => {
				if (!lastMessageId) return { head: undefined, entryHash: undefined };
				const value = await docs.get(lastMessageId);
				const head = (value as any)?.__context?.head;
				const entry = head ? await (docs as any).log.log.get(head) : undefined;
				return { head, entryHash: entry?.hash };
			},
			getMessages: () => [...messages.values()],
			peerId: canonicalPeerId,
			close: async () => {
				for (const fn of closeFns) {
					await fn();
				}
			},
		};
	} else if (scenario === "counter") {
		const peer = await PeerbitCanonicalClient.create(client, {
			adapters: [counterAdapter],
		});
		const counter = await peer.open(new CounterProgram({ id: ids.counter }), {
			args: { start: 0 },
		});
		const updateCount = async () => {
			const value = await counter.get();
			messageCountEl.textContent = String(value);
		};
		await updateCount();
		const interval = window.setInterval(() => {
			void updateCount();
		}, 250);
		closeFns.push(async () => {
			clearInterval(interval);
			await counter.close();
		});
		api = {
			scenario,
			increment: async (amount?: bigint | number) => {
				await counter.increment(amount ?? 1);
				await updateCount();
			},
			getCount: async () => {
				const value = await counter.get();
				return String(value);
			},
			peerId: canonicalPeerId,
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
			close: async () => {
				for (const fn of closeFns) {
					await fn();
				}
			},
		};
	} else {
		throw new Error(`Unknown scenario '${scenario}'`);
	}

	api.getHostStats = async () => debug.getStats();
	closeFns.push(async () => {
		client.close();
	});

	(window as any).__canonicalTest = api;
	statusEl.textContent = "ready";
	renderMessages();

	const writeParam = params.get("write");
	if (writeParam && writeParam.length > 0 && api?.put) {
		const key = `__canonical_written_${scenario}_${writeParam}`;
		if (!(window as any)[key]) {
			(window as any)[key] = true;
			for (const text of writeParam.split(",").map((s) => s.trim())) {
				if (!text) continue;
				await api.put(text);
			}
		}
	}

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
