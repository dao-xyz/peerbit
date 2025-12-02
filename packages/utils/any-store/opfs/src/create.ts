const createWorker = (directory: string) => {
	const worker = new Worker(
		new URL("/peerbit/opfs/opfs.worker.min.js#" + directory, import.meta.url),
		{ type: "classic" },
	);
	return worker;
};
export { createWorker };
