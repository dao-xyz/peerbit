const createWorker = (directory: string) => {
	const worker = new Worker(
		new URL("/peerbit/opfs/worker.js#" + directory, import.meta.url),
		{ type: "module" },
	);
	return worker;
};
export { createWorker };
