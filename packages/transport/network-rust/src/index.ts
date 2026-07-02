import { loadWasm } from "./wasm.js";

/**
 * Flat record layout returned by `decodeAndVerifyBatch`: 4 u32 words per
 * input frame. Must stay in sync with `RECORD_*` in `src/lib.rs` and the
 * consumer constants inside `@peerbit/stream`.
 *
 * word 0, byte 0: flags — bit 0 = decode ok, bit 1 = payload present
 * word 0, byte 1: top-level message variant (0 data, 1 ack, 2 hello, 3 goodbye)
 * word 0, byte 2: verify status (0 failed, 1 verified, 2 unsupported)
 * word 0, byte 3: signature count (clamped to 255)
 * word 1: header priority, or 0xffffffff when absent
 * word 2: payload byte offset into the frame (data variant only)
 * word 3: payload byte length (data variant only)
 */
export const NATIVE_WIRE_RECORD_WORDS = 4;
export const NATIVE_WIRE_FLAG_DECODE_OK = 0x01;
export const NATIVE_WIRE_FLAG_HAS_DATA = 0x02;
export const NATIVE_WIRE_NO_PRIORITY = 0xffffffff;

export enum NativeWireVerifyStatus {
	FAILED = 0,
	VERIFIED = 1,
	/** Signature scheme not natively verifiable; fall back to the TS path. */
	UNSUPPORTED = 2,
}

export type NativeWireFrameRecord = {
	decodeOk: boolean;
	variant: number;
	verifyStatus: NativeWireVerifyStatus;
	signatureCount: number;
	priority: number | undefined;
	hasData: boolean;
	dataOffset: number;
	dataLength: number;
};

/**
 * Decode one frame record out of a `decodeAndVerifyBatch` result.
 */
export const readNativeWireFrameRecord = (
	records: Uint32Array,
	index: number,
): NativeWireFrameRecord => {
	const base = index * NATIVE_WIRE_RECORD_WORDS;
	const word0 = records[base];
	const priority = records[base + 1];
	return {
		decodeOk: (word0 & NATIVE_WIRE_FLAG_DECODE_OK) !== 0,
		variant: (word0 >>> 8) & 0xff,
		verifyStatus: ((word0 >>> 16) & 0xff) as NativeWireVerifyStatus,
		signatureCount: (word0 >>> 24) & 0xff,
		priority: priority === NATIVE_WIRE_NO_PRIORITY ? undefined : priority,
		hasData: (word0 & NATIVE_WIRE_FLAG_HAS_DATA) !== 0,
		dataOffset: records[base + 2],
		dataLength: records[base + 3],
	};
};

/**
 * The native wire module surface. `decodeAndVerifyBatch` implements the
 * `NativeWire` option of `@peerbit/stream`'s DirectStream; the remaining
 * functions exist for the golden-vector parity suite and debugging.
 */
export type NativeWireModule = {
	/**
	 * Decode a batch of direct-stream frames and batch-verify their
	 * signatures (sha256-prehashed Ed25519 via ed25519-dalek). `nowMs` feeds
	 * the header expiry check. Returns NATIVE_WIRE_RECORD_WORDS u32 words
	 * per frame; see the layout above.
	 */
	decodeAndVerifyBatch(frames: Uint8Array[], nowMs: number): Uint32Array;
	/** Decode + re-encode a frame (byte-identity parity testing). */
	reencodeFrame(frame: Uint8Array): Uint8Array;
	/** Decode a frame to the stable debug-JSON parity shape. */
	decodeFrameToJson(frame: Uint8Array): string;
	/**
	 * The signable byte range of a frame — the serialization with the
	 * delivery mode and signatures excluded (they are mutated in transit).
	 */
	signableBytes(frame: Uint8Array): Uint8Array;
	/** Deterministic Rust-authored golden vectors (Rust encode → TS decode). */
	testCorpusFrames(): Uint8Array[];
};

type WireWasmExports = {
	decode_and_verify_batch(frames: Uint8Array[], nowMs: number): Uint32Array;
	reencode_frame(frame: Uint8Array): Uint8Array;
	decode_frame_to_json(frame: Uint8Array): string;
	signable_bytes(frame: Uint8Array): Uint8Array;
	test_corpus_frames(): Uint8Array[];
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
};

export const createNativeWire = async (): Promise<NativeWireModule> => {
	const wasm = await loadWasm<WireWasmExports>();
	return {
		decodeAndVerifyBatch: (frames, nowMs) =>
			wasm.decode_and_verify_batch(frames, nowMs),
		reencodeFrame: (frame) => wasm.reencode_frame(frame),
		decodeFrameToJson: (frame) => wasm.decode_frame_to_json(frame),
		signableBytes: (frame) => wasm.signable_bytes(frame),
		testCorpusFrames: () => wasm.test_corpus_frames(),
	};
};
