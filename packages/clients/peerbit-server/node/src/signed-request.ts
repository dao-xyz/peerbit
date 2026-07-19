import { BinaryWriter, deserialize, serialize } from "@dao-xyz/borsh";
import {
	type Ed25519PublicKey,
	type Identity,
	type PublicSignKey,
	SignatureWithKey,
	fromBase64,
	randomBytes,
	sha256Sync,
	toBase64,
	verify,
} from "@peerbit/crypto";
import type http from "http";

export const SIGNED_REQUEST_VERSION = "2";

export const SIGNATURE_KEY = "X-Peerbit-Signature";
export const SIGNATURE_VERSION_KEY = "X-Peerbit-Signature-Version";
export const SIGNATURE_TIME_KEY = "X-Peerbit-Signature-Time";
export const SIGNATURE_NONCE_KEY = "X-Peerbit-Signature-Nonce";
export const SIGNATURE_SERVER_KEY = "X-Peerbit-Signature-Server";
export const SIGNATURE_BOOT_KEY = "X-Peerbit-Signature-Boot";
export const SIGNED_CONTENT_LENGTH_KEY = "X-Peerbit-Content-Length";
export const SIGNED_CONTENT_SHA256_KEY = "X-Peerbit-Content-SHA256";

const REQUEST_DOMAIN = "peerbit/server/http-request/v2";
const DESCRIPTOR_DOMAIN = "peerbit/server/http-auth-descriptor/v2";
const DECIMAL_PATTERN = /^(0|[1-9][0-9]{0,15})$/;
const BASE64_32_PATTERN = /^[A-Za-z0-9+/]{43}=$/;
const METHOD_PATTERN = /^[A-Z]+$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

const DEFAULT_MAX_PAST_AGE_SECONDS = 5 * 60;
const DEFAULT_MAX_FUTURE_SKEW_SECONDS = 60;
export const DEFAULT_MAX_SIGNED_BODY_BYTES = 64 * 1024 * 1024;
const DEFAULT_MAX_REPLAY_ENTRIES = 10_000;

export class SignedRequestError extends Error {}
export class ReplayRequestError extends SignedRequestError {}
export class ReplayCapacityError extends SignedRequestError {}
export class RequestBodyTooLargeError extends SignedRequestError {}

export interface RequestAudience {
	serverPeerId: string;
	bootId: string;
}

export interface AuthDescriptor extends RequestAudience {
	version: typeof SIGNED_REQUEST_VERSION;
	serverTime: string;
	signature: string;
}

export interface VerifiedRequest {
	publicKey: PublicSignKey;
	bodyLength: number;
	bodyHash: Uint8Array;
	nonce: string;
	timestamp: bigint;
}

export interface VerifyAuthDescriptorOptions {
	nowMs?: number;
	maxPastAgeSeconds?: number;
	maxFutureSkewSeconds?: number;
}

export interface RequestAuthenticatorOptions extends RequestAudience {
	isTrusted: (key: PublicSignKey) => boolean;
	maxPastAgeSeconds?: number;
	maxFutureSkewSeconds?: number;
	maxBodyBytes?: number;
	maxReplayEntries?: number;
	wallClockMs?: () => number;
	monotonicClockMs?: () => number;
}

const frame = (domain: string, fields: readonly string[]): Uint8Array => {
	const writer = new BinaryWriter();
	writer.string(domain);
	for (const value of fields) {
		writer.string(value);
	}
	return writer.finalize();
};

const requestFrame = (properties: {
	serverPeerId: string;
	bootId: string;
	timestamp: string;
	nonce: string;
	method: string;
	target: string;
	bodyLength: string;
	bodyHash: string;
}): Uint8Array =>
	frame(REQUEST_DOMAIN, [
		SIGNED_REQUEST_VERSION,
		properties.serverPeerId,
		properties.bootId,
		properties.timestamp,
		properties.nonce,
		properties.method,
		properties.target,
		properties.bodyLength,
		properties.bodyHash,
	]);

const descriptorFrame = (descriptor: Omit<AuthDescriptor, "signature">) =>
	frame(DESCRIPTOR_DOMAIN, [
		descriptor.version,
		descriptor.serverPeerId,
		descriptor.bootId,
		descriptor.serverTime,
	]);

const bodyBytes = (data: string | Uint8Array | undefined): Uint8Array => {
	if (data == null) return new Uint8Array();
	return typeof data === "string" ? encoder.encode(data) : data;
};

const parseCanonicalUnsigned = (
	value: string,
	name: string,
	maximum?: bigint,
): bigint => {
	if (!DECIMAL_PATTERN.test(value)) {
		throw new SignedRequestError(`Invalid ${name}`);
	}
	const parsed = BigInt(value);
	if (maximum != null && parsed > maximum) {
		throw new SignedRequestError(`Invalid ${name}`);
	}
	return parsed;
};

const decodeCanonical32 = (value: string, name: string): Uint8Array => {
	if (!BASE64_32_PATTERN.test(value)) {
		throw new SignedRequestError(`Invalid ${name}`);
	}
	const bytes = fromBase64(value);
	if (bytes.length !== 32 || toBase64(bytes) !== value) {
		throw new SignedRequestError(`Invalid ${name}`);
	}
	return bytes;
};

const readHeader = (
	headers: Record<string, string | string[] | undefined>,
	name: string,
	maxLength: number,
): string => {
	const matches = Object.entries(headers).filter(
		([key]) => key.toLowerCase() === name.toLowerCase(),
	);
	if (matches.length !== 1 || typeof matches[0][1] !== "string") {
		throw new SignedRequestError(`Missing or duplicate ${name} header`);
	}
	const value = matches[0][1] as string;
	if (value.length === 0 || value.length > maxLength || value.includes(",")) {
		throw new SignedRequestError(`Invalid ${name} header`);
	}
	return value;
};

const validateMethodAndTarget = (method: string, target: string) => {
	if (!METHOD_PATTERN.test(method)) {
		throw new SignedRequestError("Invalid request method");
	}
	if (!target.startsWith("/") || target.includes("#") || target.length > 8192) {
		throw new SignedRequestError("Invalid request target");
	}
};

const equalBytes = (left: Uint8Array, right: Uint8Array) => {
	if (left.length !== right.length) return false;
	let difference = 0;
	for (let index = 0; index < left.length; index++) {
		difference |= left[index] ^ right[index];
	}
	return difference === 0;
};

export const createAuthDescriptor = async (
	identity: Identity<Ed25519PublicKey>,
	audience: RequestAudience,
	nowMs: number = Date.now(),
): Promise<AuthDescriptor> => {
	decodeCanonical32(audience.bootId, "boot ID");
	if (!audience.serverPeerId || audience.serverPeerId.length > 256) {
		throw new SignedRequestError("Invalid server peer ID");
	}
	const unsigned: Omit<AuthDescriptor, "signature"> = {
		version: SIGNED_REQUEST_VERSION,
		serverPeerId: audience.serverPeerId,
		bootId: audience.bootId,
		serverTime: Math.floor(nowMs / 1000).toString(),
	};
	const signature = await identity.sign(descriptorFrame(unsigned));
	return { ...unsigned, signature: toBase64(serialize(signature)) };
};

export const verifyAuthDescriptor = async (
	value: unknown,
	expectedServerPeerId: string,
	options?: VerifyAuthDescriptorOptions,
): Promise<AuthDescriptor> => {
	if (!value || typeof value !== "object") {
		throw new SignedRequestError("Invalid authentication descriptor");
	}
	const input = value as Partial<AuthDescriptor>;
	if (
		input.version !== SIGNED_REQUEST_VERSION ||
		typeof input.serverPeerId !== "string" ||
		typeof input.bootId !== "string" ||
		typeof input.serverTime !== "string" ||
		typeof input.signature !== "string"
	) {
		throw new SignedRequestError("Invalid authentication descriptor");
	}
	if (
		input.serverPeerId !== expectedServerPeerId ||
		input.serverPeerId.length > 256
	) {
		throw new SignedRequestError("Server peer ID does not match the pinned ID");
	}
	decodeCanonical32(input.bootId, "boot ID");
	const serverTime = parseCanonicalUnsigned(input.serverTime, "server time");
	const nowMs = options?.nowMs ?? Date.now();
	const maxPastAgeSeconds =
		options?.maxPastAgeSeconds ?? DEFAULT_MAX_PAST_AGE_SECONDS;
	const maxFutureSkewSeconds =
		options?.maxFutureSkewSeconds ?? DEFAULT_MAX_FUTURE_SKEW_SECONDS;
	if (
		!Number.isFinite(nowMs) ||
		nowMs < 0 ||
		nowMs > Number.MAX_SAFE_INTEGER ||
		!Number.isSafeInteger(maxPastAgeSeconds) ||
		maxPastAgeSeconds < 0 ||
		!Number.isSafeInteger(maxFutureSkewSeconds) ||
		maxFutureSkewSeconds < 0
	) {
		throw new SignedRequestError("Invalid descriptor freshness policy");
	}
	const now = BigInt(Math.floor(nowMs / 1000));
	if (
		serverTime < now - BigInt(maxPastAgeSeconds) ||
		serverTime > now + BigInt(maxFutureSkewSeconds)
	) {
		throw new SignedRequestError(
			"Authentication descriptor timestamp is outside the window",
		);
	}
	if (input.signature.length > 2048 || input.signature.includes(",")) {
		throw new SignedRequestError("Invalid descriptor signature");
	}
	let signature: SignatureWithKey;
	try {
		const bytes = fromBase64(input.signature);
		if (toBase64(bytes) !== input.signature) {
			throw new Error("Non-canonical base64");
		}
		signature = deserialize(bytes, SignatureWithKey);
		if (!equalBytes(serialize(signature), bytes)) {
			throw new Error("Non-canonical signature encoding");
		}
	} catch {
		throw new SignedRequestError("Invalid descriptor signature");
	}
	if (signature.publicKey.toPeerId().toString() !== input.serverPeerId) {
		throw new SignedRequestError(
			"Descriptor signer does not match server peer ID",
		);
	}
	const descriptor: AuthDescriptor = input as AuthDescriptor;
	if (!(await verify(signature, descriptorFrame(descriptor)))) {
		throw new SignedRequestError("Invalid descriptor signature");
	}
	return descriptor;
};

export const signRequest = async (
	headers: Record<string, string>,
	method: string,
	target: string,
	data: string | Uint8Array | undefined,
	keypair: Identity<Ed25519PublicKey>,
	audience: RequestAudience,
	options?: { nowMs?: number; nonce?: Uint8Array },
) => {
	validateMethodAndTarget(method, target);
	if (!audience.serverPeerId || audience.serverPeerId.length > 256) {
		throw new SignedRequestError("Invalid server peer ID");
	}
	decodeCanonical32(audience.bootId, "boot ID");
	const bytes = bodyBytes(data);
	const timestamp = Math.floor(
		(options?.nowMs ?? Date.now()) / 1000,
	).toString();
	const nonceBytes = options?.nonce ?? randomBytes(32);
	if (nonceBytes.length !== 32) {
		throw new SignedRequestError("Nonce must contain 32 bytes");
	}
	const nonce = toBase64(nonceBytes);
	const length = bytes.length.toString();
	const hash = toBase64(sha256Sync(bytes));
	const signable = requestFrame({
		...audience,
		timestamp,
		nonce,
		method,
		target,
		bodyLength: length,
		bodyHash: hash,
	});
	const signature = await keypair.sign(signable);
	headers[SIGNATURE_VERSION_KEY] = SIGNED_REQUEST_VERSION;
	headers[SIGNATURE_TIME_KEY] = timestamp;
	headers[SIGNATURE_NONCE_KEY] = nonce;
	headers[SIGNATURE_SERVER_KEY] = audience.serverPeerId;
	headers[SIGNATURE_BOOT_KEY] = audience.bootId;
	headers[SIGNED_CONTENT_LENGTH_KEY] = length;
	headers[SIGNED_CONTENT_SHA256_KEY] = hash;
	headers[SIGNATURE_KEY] = toBase64(serialize(signature));
};

export class RequestAuthenticator {
	readonly maxBodyBytes: number;
	private readonly maxPastAgeSeconds: bigint;
	private readonly maxFutureSkewSeconds: bigint;
	private readonly maxReplayEntries: number;
	private readonly wallClockMs: () => number;
	private readonly monotonicClockMs: () => number;
	private readonly replays = new Map<
		string,
		{ monotonicUntil: number; wallUntil: bigint }
	>();
	private lastWallSeconds?: bigint;

	constructor(readonly options: RequestAuthenticatorOptions) {
		this.maxPastAgeSeconds = BigInt(
			options.maxPastAgeSeconds ?? DEFAULT_MAX_PAST_AGE_SECONDS,
		);
		this.maxFutureSkewSeconds = BigInt(
			options.maxFutureSkewSeconds ?? DEFAULT_MAX_FUTURE_SKEW_SECONDS,
		);
		this.maxBodyBytes = options.maxBodyBytes ?? DEFAULT_MAX_SIGNED_BODY_BYTES;
		this.maxReplayEntries =
			options.maxReplayEntries ?? DEFAULT_MAX_REPLAY_ENTRIES;
		this.wallClockMs = options.wallClockMs ?? Date.now;
		this.monotonicClockMs =
			options.monotonicClockMs ?? (() => performance.now());
		if (
			this.maxPastAgeSeconds < 0n ||
			this.maxFutureSkewSeconds < 0n ||
			!Number.isSafeInteger(this.maxBodyBytes) ||
			this.maxBodyBytes < 0 ||
			!Number.isSafeInteger(this.maxReplayEntries) ||
			this.maxReplayEntries < 1
		) {
			throw new SignedRequestError("Invalid signed-request policy");
		}
		decodeCanonical32(options.bootId, "boot ID");
	}

	private effectiveWallSeconds(): bigint {
		const current = BigInt(Math.floor(this.wallClockMs() / 1000));
		if (this.lastWallSeconds == null || current > this.lastWallSeconds) {
			this.lastWallSeconds = current;
		}
		return this.lastWallSeconds;
	}

	private precheckReplay(
		publicKey: PublicSignKey,
		nonce: string,
		wallNow: bigint,
	) {
		const monotonicNow = this.monotonicClockMs();
		const key = `${publicKey.hashcode()}\0${nonce}`;
		const expired = (entry: { monotonicUntil: number; wallUntil: bigint }) =>
			monotonicNow >= entry.monotonicUntil && wallNow > entry.wallUntil;
		const existing = this.replays.get(key);
		if (existing != null && !expired(existing)) {
			throw new ReplayRequestError("Request has already been used");
		}
		if (this.replays.size < this.maxReplayEntries || existing != null) return;
		for (const entry of this.replays.values()) {
			if (expired(entry)) return;
		}
		throw new ReplayCapacityError("Replay protection is at capacity");
	}

	private consumeReplay(
		publicKey: PublicSignKey,
		nonce: string,
		timestamp: bigint,
		wallNow: bigint,
	) {
		const now = this.monotonicClockMs();
		const key = `${publicKey.hashcode()}\0${nonce}`;
		const existing = this.replays.get(key);
		const expired = (entry: { monotonicUntil: number; wallUntil: bigint }) =>
			now >= entry.monotonicUntil && wallNow > entry.wallUntil;
		if (existing != null && !expired(existing)) {
			throw new ReplayRequestError("Request has already been used");
		}
		if (existing != null) this.replays.delete(key);
		if (this.replays.size >= this.maxReplayEntries) {
			for (const [candidate, entry] of this.replays) {
				if (expired(entry)) this.replays.delete(candidate);
			}
		}
		if (this.replays.size >= this.maxReplayEntries) {
			throw new ReplayCapacityError("Replay protection is at capacity");
		}
		const retentionSeconds =
			Number(this.maxPastAgeSeconds + this.maxFutureSkewSeconds) + 1;
		this.replays.set(key, {
			monotonicUntil: now + retentionSeconds * 1000,
			wallUntil: timestamp + this.maxPastAgeSeconds,
		});
	}

	/**
	 * Atomically consume a verified nonce immediately before dispatch. Call this
	 * only after the exact request body has passed verifyRequestBody().
	 */
	consume(verified: VerifiedRequest) {
		const now = this.effectiveWallSeconds();
		if (
			verified.timestamp < now - this.maxPastAgeSeconds ||
			verified.timestamp > now + this.maxFutureSkewSeconds
		) {
			throw new SignedRequestError("Request timestamp is outside the window");
		}
		if (!this.options.isTrusted(verified.publicKey)) {
			throw new SignedRequestError("Request signer is not trusted");
		}
		this.consumeReplay(
			verified.publicKey,
			verified.nonce,
			verified.timestamp,
			now,
		);
	}

	async verify(
		headers: Record<string, string | string[] | undefined>,
		method: string,
		target: string,
	): Promise<VerifiedRequest> {
		validateMethodAndTarget(method, target);
		const version = readHeader(headers, SIGNATURE_VERSION_KEY, 8);
		if (version !== SIGNED_REQUEST_VERSION) {
			throw new SignedRequestError("Unsupported signed-request version");
		}
		const timestampValue = readHeader(headers, SIGNATURE_TIME_KEY, 16);
		const timestamp = parseCanonicalUnsigned(timestampValue, "timestamp");
		const now = this.effectiveWallSeconds();
		if (
			timestamp < now - this.maxPastAgeSeconds ||
			timestamp > now + this.maxFutureSkewSeconds
		) {
			throw new SignedRequestError("Request timestamp is outside the window");
		}
		const nonce = readHeader(headers, SIGNATURE_NONCE_KEY, 44);
		decodeCanonical32(nonce, "nonce");
		const serverPeerId = readHeader(headers, SIGNATURE_SERVER_KEY, 256);
		const bootId = readHeader(headers, SIGNATURE_BOOT_KEY, 44);
		if (
			serverPeerId !== this.options.serverPeerId ||
			bootId !== this.options.bootId
		) {
			throw new SignedRequestError(
				"Request audience does not match this server",
			);
		}
		decodeCanonical32(bootId, "boot ID");
		const bodyLengthValue = readHeader(headers, SIGNED_CONTENT_LENGTH_KEY, 16);
		const bodyLengthBig = parseCanonicalUnsigned(
			bodyLengthValue,
			"body length",
			BigInt(this.maxBodyBytes),
		);
		const bodyLength = Number(bodyLengthBig);
		const bodyHashValue = readHeader(headers, SIGNED_CONTENT_SHA256_KEY, 44);
		const bodyHash = decodeCanonical32(bodyHashValue, "body hash");
		const signatureValue = readHeader(headers, SIGNATURE_KEY, 2048);
		let signature: SignatureWithKey;
		try {
			const signatureBytes = fromBase64(signatureValue);
			if (toBase64(signatureBytes) !== signatureValue) {
				throw new Error("Non-canonical base64");
			}
			signature = deserialize(signatureBytes, SignatureWithKey);
			if (!equalBytes(serialize(signature), signatureBytes)) {
				throw new Error("Non-canonical signature encoding");
			}
		} catch {
			throw new SignedRequestError("Invalid request signature encoding");
		}
		if (!this.options.isTrusted(signature.publicKey)) {
			throw new SignedRequestError("Request signer is not trusted");
		}
		const signable = requestFrame({
			serverPeerId,
			bootId,
			timestamp: timestampValue,
			nonce,
			method,
			target,
			bodyLength: bodyLengthValue,
			bodyHash: bodyHashValue,
		});
		if (!(await verify(signature, signable))) {
			throw new SignedRequestError("Invalid request signature");
		}
		// Reject already-committed replays and a full cache before reading a
		// potentially large body. This check is deliberately non-mutating; consume()
		// remains the authoritative atomic check-and-insert after body verification.
		this.precheckReplay(
			signature.publicKey,
			nonce,
			this.effectiveWallSeconds(),
		);
		return {
			publicKey: signature.publicKey,
			bodyLength,
			bodyHash,
			nonce,
			timestamp,
		};
	}
}

export const getBody = (
	req: http.IncomingMessage,
	maxBytes: number = DEFAULT_MAX_SIGNED_BODY_BYTES,
): Promise<Uint8Array> => {
	return new Promise((resolve, reject) => {
		const chunks: Uint8Array[] = [];
		let length = 0;
		let settled = false;
		const fail = (error: Error) => {
			if (settled) return;
			settled = true;
			chunks.length = 0;
			reject(error);
		};
		req.on("data", (chunk: Uint8Array | string) => {
			if (settled) return;
			const bytes =
				typeof chunk === "string"
					? encoder.encode(chunk)
					: new Uint8Array(chunk);
			length += bytes.length;
			if (length > maxBytes) {
				fail(new RequestBodyTooLargeError("Request body is too large"));
				return;
			}
			chunks.push(bytes);
		});
		req.on("end", () => {
			if (settled) return;
			settled = true;
			const body = new Uint8Array(length);
			let offset = 0;
			for (const chunk of chunks) {
				body.set(chunk, offset);
				offset += chunk.length;
			}
			resolve(body);
		});
		req.on("aborted", () =>
			fail(new SignedRequestError("Request was aborted")),
		);
		req.on("error", fail);
	});
};

export const verifyRequestBody = (
	verified: VerifiedRequest,
	body: Uint8Array,
): string => {
	if (
		body.length !== verified.bodyLength ||
		!equalBytes(sha256Sync(body), verified.bodyHash)
	) {
		throw new SignedRequestError("Request body does not match its signature");
	}
	try {
		return decoder.decode(body);
	} catch {
		throw new SignedRequestError("Request body is not valid UTF-8");
	}
};
