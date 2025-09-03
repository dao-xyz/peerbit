export { createSharedWorkerClient } from "./client.js";
// Note: host.ts is a worker entry and not meant to be imported directly in app code.
// Bundlers will include it when referenced via new URL('./sharedworker/host.ts', import.meta.url)
