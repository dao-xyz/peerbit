const createWorker = (directory: string) => new Worker(new URL("/peerbit/anystore-opfs-worker.min.js#" + directory, import.meta.url), { type: "classic" })
export { createWorker }