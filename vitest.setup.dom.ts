// vitest.setup.dom.ts
// Runs before each test file in the jsdom project
// 1) TextEncoder/TextDecoder
import { TextDecoder, TextEncoder } from "node:util";

(globalThis as any).TextEncoder = TextEncoder;
globalThis.TextDecoder = TextDecoder as typeof globalThis.TextDecoder;

/* // 2) Web Crypto (getRandomValues, subtle, etc.)
import { webcrypto } from 'node:crypto';
if (typeof globalThis.crypto === 'undefined') {
    globalThis.crypto = webcrypto as unknown as Crypto;
}

// 3) atob/btoa for some libs (optional)
if (typeof globalThis.atob === 'undefined') {
    globalThis.atob = (data: string) =>
        Buffer.from(data, 'base64').toString('binary');
}
if (typeof globalThis.btoa === 'undefined') {
    globalThis.btoa = (data: string) =>
        Buffer.from(data, 'binary').toString('base64');
}

// 4) (Peerbit / IndexedDB heavy code) – optional but handy
// import 'fake-indexeddb/auto';

// 5) Optional: normalize cross-realm Uint8Array at a single boundary.
//    If you have a single helper, expose it globally for tests.
(globalThis as any).__asU8 = function asU8(x: unknown): Uint8Array {
    if (x instanceof Uint8Array) return x;
    if (ArrayBuffer.isView(x)) {
        const v = x as ArrayBufferView;
        return new Uint8Array(v.buffer, v.byteOffset, v.byteLength); // zero-copy
    }
    if (x instanceof ArrayBuffer) return new Uint8Array(x);
    if (typeof Buffer !== 'undefined' && x instanceof Buffer) {
        return new Uint8Array(x.buffer, x.byteOffset, x.byteLength);
    }
    if (typeof x === 'string') return new TextEncoder().encode(x);
    if (Array.isArray(x)) return new Uint8Array(x);
    throw new TypeError('Unsupported input to asU8');
}; */
