import sodium from "libsodium-wrappers";
import { base64 } from "multiformats/bases/base64";
export const fromHexString = (hexString: string) =>
    Uint8Array.from(
        hexString.match(/.{1,2}/g)!.map((byte) => parseInt(byte, 16))
    );

export const toHexString = (bytes: Uint8Array) =>
    bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, "0"), "");

export const toBase64 = async (arr: Uint8Array) => {
    await sodium.ready;
    return sodium.to_base64(arr);
};
export const fromBase64 = async (base64: string) => {
    await sodium.ready;
    return sodium.from_base64(base64);
};

export const toBase64Sync = (arr: Uint8Array) => {
    return base64.encode(arr);
};
export const fromBase64Sync = (str: string) => {
    return base64.decode(str);
};
