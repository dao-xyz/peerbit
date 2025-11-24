const createWorker = (directory: string) => {
	const worker = new Worker(
		new URL("/peerbit/opfs/worker.js#" + directory, import.meta.url),
		{ type: "classic" },
	);
	return worker;
};
export { createWorker };
