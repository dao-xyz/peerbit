import { createStore, type RustAnyStore } from "../../src/index.js";

type WorkerRequest = {
	id: number;
	op: string;
	directory?: string;
	level?: string;
	key?: string;
	keys?: string[];
	entries?: [string, string][];
	value?: string;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

let store: RustAnyStore | undefined;
const sublevels = new Map<string, Awaited<ReturnType<RustAnyStore["sublevel"]>>>();

const encodePathPart = (part: string): string =>
	encodeURIComponent(part).replace(/[!'()*]/g, (char) =>
		`%${char.charCodeAt(0).toString(16).toUpperCase()}`,
	);

const requireStore = (): RustAnyStore => {
	if (!store) {
		throw new Error("store is not open");
	}
	return store;
};

const getSublevel = async (level: string) => {
	const existing = sublevels.get(level);
	if (existing) {
		return existing;
	}
	const sublevel = await requireStore().sublevel(level);
	sublevels.set(level, sublevel);
	return sublevel;
};

const writeOpfsFile = async (
	directory: string,
	name: string,
	value: string,
): Promise<void> => {
	const storage = await navigator.storage.getDirectory();
	const root = await storage.getDirectoryHandle(encodePathPart(directory), {
		create: true,
	});
	const file = await root.getFileHandle(name, { create: true });
	const handle = await file.createSyncAccessHandle();
	try {
		const bytes = encoder.encode(value);
		handle.write(bytes, { at: 0 });
		handle.truncate(bytes.byteLength);
		handle.flush();
	} finally {
		handle.close();
	}
};

const handle = async (message: WorkerRequest): Promise<unknown> => {
	if (message.op === "open") {
		if (!message.directory) {
			throw new Error("open requires directory");
		}
		store = createStore(message.directory);
		sublevels.clear();
		await store.open();
		return undefined;
	}
	if (message.op === "close") {
		await store?.close();
		store = undefined;
		sublevels.clear();
		return undefined;
	}
	if (message.op === "put") {
		await requireStore().put(message.key!, encoder.encode(message.value ?? ""));
		return undefined;
	}
	if (message.op === "get") {
		const bytes = await requireStore().get(message.key!);
		return bytes ? decoder.decode(bytes) : undefined;
	}
	if (message.op === "del") {
		await requireStore().del(message.key!);
		return undefined;
	}
	if (message.op === "putMany") {
		await requireStore().putMany(
			message.entries!.map(([key, value]) => [key, encoder.encode(value)]),
		);
		return undefined;
	}
	if (message.op === "getMany") {
		const values = await requireStore().getMany(message.keys!);
		return values.map((bytes) => (bytes ? decoder.decode(bytes) : undefined));
	}
	if (message.op === "delMany") {
		return requireStore().delMany(message.keys!);
	}
	if (message.op === "subPut") {
		const sublevel = await getSublevel(message.level!);
		await sublevel.put(message.key!, encoder.encode(message.value ?? ""));
		return undefined;
	}
	if (message.op === "subGet") {
		const sublevel = await getSublevel(message.level!);
		const bytes = await sublevel.get(message.key!);
		return bytes ? decoder.decode(bytes) : undefined;
	}
	if (message.op === "listKeys") {
		const keys: string[] = [];
		for await (const [key] of requireStore().iterator()) {
			keys.push(key);
		}
		return keys.sort();
	}
	if (message.op === "size") {
		return requireStore().size();
	}
	if (message.op === "clear") {
		await requireStore().clear();
		return undefined;
	}
	if (message.op === "writeOpfsFile") {
		if (!message.directory || !message.key) {
			throw new Error("writeOpfsFile requires directory and key");
		}
		await writeOpfsFile(message.directory, message.key, message.value ?? "");
		return undefined;
	}
	throw new Error(`Unknown op '${message.op}'`);
};

self.addEventListener("message", (event: MessageEvent<WorkerRequest>) => {
	void handle(event.data)
		.then((value) => {
			self.postMessage({ id: event.data.id, ok: true, value });
		})
		.catch((error) => {
			self.postMessage({
				id: event.data.id,
				ok: false,
				error: error?.message || String(error),
			});
		});
});
