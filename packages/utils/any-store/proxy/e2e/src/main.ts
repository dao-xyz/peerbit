import { openAnyStore } from "@peerbit/any-store-proxy/client";
import {
	CanonicalClient,
	connectServiceWorker,
} from "@peerbit/canonical-client";
import { connectSharedWorker } from "@peerbit/canonical-client";

const statusEl = document.querySelector(
	'[data-testid="status"]',
) as HTMLElement;
const peerIdEl = document.querySelector(
	'[data-testid="peer-id"]',
) as HTMLElement;

const params = new URLSearchParams(window.location.search);
const scenario = params.get("scenario") || "any-store";
const session = params.get("session") || "default";
const transport = params.get("transport") || "sharedworker";
const keepAliveIntervalMs = Number(params.get("keepAliveIntervalMs") ?? "2000");
const keepAliveTimeoutMs = Number(params.get("keepAliveTimeoutMs") ?? "10000");
const keepAliveFailures = Number(params.get("keepAliveFailures") ?? "2");

const connectSharedWorkerClient = async (): Promise<{
	client: CanonicalClient;
}> => {
	const worker = new SharedWorker(new URL("./worker.ts", import.meta.url), {
		name: `peerbit-canonical-any-store-${session}`,
		type: "module",
	});
	worker.port.start();
	const client = await connectSharedWorker(worker);
	return { client };
};

const init = async () => {
	statusEl.textContent = "connecting";
	let api: any;
	let client: CanonicalClient | undefined;

	if (transport === "serviceworker") {
		statusEl.textContent = "connecting (service worker)";
		client = await connectServiceWorker({
			url: new URL("../service-worker.ts", import.meta.url),
			scope: "/",
			type: "module",
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

	const canonicalPeerId = await client.peerId();
	peerIdEl.textContent = canonicalPeerId;

	if (scenario === "any-store") {
		const store = await openAnyStore({ client });
		const encoder = new TextEncoder();
		const decoder = new TextDecoder();
		const sublevels = new Map<
			string,
			Awaited<ReturnType<typeof store.sublevel>>
		>();

		const getSub = async (name: string) => {
			const existing = sublevels.get(name);
			if (existing) return existing;
			const sub = await store.sublevel(name);
			sublevels.set(name, sub as any);
			return sub as any;
		};

		api = {
			scenario,
			peerId: canonicalPeerId,
			put: async (key: string, value: string) => {
				await store.put(key, encoder.encode(value));
			},
			get: async (key: string) => {
				const bytes = await store.get(key);
				return bytes ? decoder.decode(bytes) : undefined;
			},
			listKeys: async () => {
				const keys: string[] = [];
				for await (const [key] of store.iterator()) {
					keys.push(key);
				}
				return keys;
			},
			subPut: async (level: string, key: string, value: string) => {
				const sub = await getSub(level);
				await sub.put(key, encoder.encode(value));
			},
			subGet: async (level: string, key: string) => {
				const sub = await getSub(level);
				const bytes = await sub.get(key);
				return bytes ? decoder.decode(bytes) : undefined;
			},
			close: async () => {
				try {
					store.closePort();
				} finally {
					stopKeepAlive();
					client.close();
				}
			},
		};
	} else {
		throw new Error(`Unknown scenario '${scenario}'`);
	}

	(window as any).__canonicalTest = api;
	statusEl.textContent = "ready";
};

void init().catch((error) => {
	console.error("canonical e2e init failed", error);
	statusEl.textContent = `error: ${error?.message || String(error)}`;
});
