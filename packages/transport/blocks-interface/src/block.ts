import * as dagCbor from "@ipld/dag-cbor";
import { CID } from "multiformats";
import { base58btc } from "multiformats/bases/base58";
import { type Block, createUnsafe, encode } from "multiformats/block";
import * as raw from "multiformats/codecs/raw";
import type { MultihashHasher } from "multiformats/hashes/interface";
import { sha256 } from "multiformats/hashes/sha2";

const unsupportedCodecError = () => new Error("unsupported codec");

const defaultBase = base58btc;

export const defaultHasher = sha256;

export const codecCodes = {
	[dagCbor.code]: dagCbor,
	[raw.code]: raw,
};
export const codecMap = {
	raw,
	"dag-cbor": dagCbor,
};

export type VerifyBlockBytesOptions = {
	hasher?: MultihashHasher<number>;
	/**
	 * The codec expected by the caller. Only its multicodec code is consulted;
	 * verification deliberately does not decode untrusted block bytes.
	 */
	codec?: { code: number };
};

export const cidifyString = (str: string): CID => {
	if (!str) {
		return str as any as CID; // TODO fix types
	}

	return CID.parse(str, defaultBase);
};

export const stringifyCid = (cid: any): string => {
	if (!cid || typeof cid === "string") {
		return cid;
	}

	if (cid["/"]) {
		return defaultBase.encode(cid["/"]);
	}
	return cid.toString(defaultBase);
};

/**
 * Verify that bytes match a CID without decoding their logical value.
 *
 * This is the appropriate boundary for transports and block stores that only
 * move opaque bytes. In particular, it prevents a mismatching DAG-CBOR payload
 * from allocating a decoded object graph before its digest is rejected.
 */
export const verifyBlockBytes = async (
	expectedCID: CID | string,
	bytes: Uint8Array,
	options: VerifyBlockBytesOptions = {},
): Promise<CID> => {
	const cidObject =
		typeof expectedCID === "string" ? cidifyString(expectedCID) : expectedCID;
	const codec =
		options.codec || codecCodes[cidObject.code as keyof typeof codecCodes];
	if (!codec) {
		throw unsupportedCodecError();
	}
	if (codec.code !== cidObject.code) {
		throw new Error("CID codec does not match");
	}

	const digest = await (options.hasher || defaultHasher).digest(bytes);
	const resolved = CID.create(cidObject.version, codec.code, digest);
	if (!resolved.equals(cidObject)) {
		throw new Error("CID does not match");
	}
	return resolved;
};

export const checkDecodeBlock = async (
	expectedCID: CID | string,
	bytes: Uint8Array,
	options: { hasher?: any; codec?: any },
): Promise<Block<any, any, any, any>> => {
	const cidObject =
		typeof expectedCID === "string" ? cidifyString(expectedCID) : expectedCID;
	const codec =
		options.codec || codecCodes[cidObject.code as keyof typeof codecCodes];
	const resolved = await verifyBlockBytes(cidObject, bytes, {
		codec,
		hasher: options.hasher,
	});
	if (codec?.code === raw.code) {
		return {
			bytes,
			cid: resolved,
			value: bytes,
		} as Block<Uint8Array, typeof raw.code, number, 1>;
	}
	return createUnsafe({
		bytes,
		cid: resolved,
		codec,
	}) as Block<any, any, any, 1>;
};
export const getBlockValue = async <T>(
	block: Block<T, any, any, any>,
	links?: string[],
): Promise<T> => {
	if (block.cid.code === dagCbor.code) {
		const value = block.value as any;
		links = links || [];
		links.forEach((prop) => {
			if (value[prop]) {
				value[prop] = Array.isArray(value[prop])
					? value[prop].map(stringifyCid)
					: stringifyCid(value[prop]);
			}
		});
		return value;
	}
	if (block.cid.code === raw.code) {
		return block.value as T;
	}
	throw new Error("Unsupported");
};

export const prepareBlockWrite = (value: any, codec: any, links?: string[]) => {
	if (!codec) throw unsupportedCodecError();
	if (codec.code === dagCbor.code) {
		links = links || [];
		links.forEach((prop) => {
			if (value[prop]) {
				value[prop] = Array.isArray(value[prop])
					? value[prop].map(cidifyString)
					: cidifyString(value[prop]);
			}
		});
	}
	return value;
};

export const createBlock = async (
	value: any,
	format: string,
	options?: {
		hasher?: MultihashHasher<number>;
		links?: string[];
	},
): Promise<Block<any, any, any, any>> => {
	const codec = (codecMap as any)[format];
	value = prepareBlockWrite(value, codec, options?.links);
	const block = await encode({
		value,
		codec,
		hasher: options?.hasher || defaultHasher,
	});
	return block as Block<any, any, any, any>;
};

export const calculateRawCid = async (bytes: Uint8Array) => {
	const digest = await defaultHasher.digest(bytes);
	const cid = CID.createV1(raw.code, digest);
	const block = {
		bytes,
		cid,
		value: bytes,
	} as Block<Uint8Array, typeof raw.code, number, 1>;
	return {
		block,
		cid: stringifyCid(cid),
	};
};
