export const randomBytes = (len: number) =>
	globalThis.crypto.getRandomValues(new Uint8Array(len));
