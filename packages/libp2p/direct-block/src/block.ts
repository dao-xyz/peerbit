import { CID } from "multiformats/cid";
import * as raw from "multiformats/codecs/raw";
import * as dagCbor from "@ipld/dag-cbor";
import { sha256 } from "multiformats/hashes/sha2";
import { base58btc } from "multiformats/bases/base58";
import * as Block from "multiformats/block";
import type { MultihashHasher } from "multiformats/hashes/hasher";

const unsupportedCodecError = () => new Error("unsupported codec");

const defaultBase = base58btc;

export const defaultHasher = sha256;

export const codecCodes = {
	[dagCbor.code]: dagCbor,
	[raw.code]: raw,
};
export const codecMap = {
	raw: raw,
	"dag-cbor": dagCbor,
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

export const checkDecodeBlock = async (
	expectedCID: CID | string,
	bytes: Uint8Array,
	options: { hasher?: any; codec?: any }
): Promise<Block.Block<any, any, any, any>> => {
	const cidObject =
		typeof expectedCID === "string" ? cidifyString(expectedCID) : expectedCID;
	const codec = options.codec || codecCodes[cidObject.code];
	const block = await Block.decode({
		bytes: bytes,
		codec,
		hasher: options?.hasher || defaultHasher,
	});
	if (!block.cid.equals(cidObject)) {
		throw new Error("CID does not match");
	}
	return block as Block.Block<any, any, any, 1>;
};
export const getBlockValue = async <T>(
	block: Block.Block<T, any, any, any>,
	links?: string[]
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
	}
): Promise<Block.Block<any, any, any, any>> => {
	const codec = codecMap[format];
	value = prepareBlockWrite(value, codec, options?.links);
	const block = await Block.encode({
		value,
		codec,
		hasher: options?.hasher || defaultHasher,
	});
	return block as Block.Block<any, any, any, any>;
};
