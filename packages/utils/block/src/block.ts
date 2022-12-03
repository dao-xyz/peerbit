import { CID } from "multiformats/cid";
import * as raw from "multiformats/codecs/raw";
import * as dagCbor from "@ipld/dag-cbor";
import sodium from "libsodium-wrappers";
import { from } from "multiformats/hashes/hasher";
import { base58btc } from "multiformats/bases/base58";

const defaultBase = base58btc;

export const blake2b = from({
    name: "blake2b",
    code: 0x42,
    encode: async (input) => {
        await sodium.ready;
        return sodium.crypto_generichash(32, input);
    },
});
export const defaultHasher = blake2b;
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

    return CID.parse(str);
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
