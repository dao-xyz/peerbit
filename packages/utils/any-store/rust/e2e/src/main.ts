type WorkerRequest =
	| { op: "open"; directory: string }
	| { op: "close" | "listKeys" | "size" | "clear" }
	| { op: "put" | "get" | "del" | "writeOpfsFile"; key: string; value?: string }
	| { op: "subPut" | "subGet"; level: string; key: string; value?: string };

type WorkerResponse =
	| { id: number; ok: true; value?: unknown }
	| { id: number; ok: false; error: string };

const statusEl = document.querySelector(
	'[data-testid="status"]',
) as HTMLElement;

let nextId = 0;
let worker: Worker;
let pending = new Map<
	number,
	{ resolve: (value: unknown) => void; reject: (error: Error) => void }
>();

const createWorker = () => {
	const next = new Worker(new URL("./worker.ts", import.meta.url), {
		type: "module",
	});
	next.addEventListener("message", (event: MessageEvent<WorkerResponse>) => {
		const response = event.data;
		const callbacks = pending.get(response.id);
		if (!callbacks) {
			return;
		}
		pending.delete(response.id);
		if (response.ok) {
			callbacks.resolve(response.value);
		} else {
			callbacks.reject(new Error(response.error));
		}
	});
	return next;
};

const request = (message: WorkerRequest): Promise<unknown> => {
	const id = nextId++;
	return new Promise((resolve, reject) => {
		pending.set(id, { resolve, reject });
		worker.postMessage({ id, ...message });
	});
};

const restartWorker = async () => {
	pending.forEach(({ reject }) => reject(new Error("worker restarted")));
	pending = new Map();
	worker.terminate();
	worker = createWorker();
};

worker = createWorker();

(window as any).__rustAnyStoreTest = {
	request,
	restartWorker,
};

statusEl.textContent = "ready";
