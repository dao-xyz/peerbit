import sodium from "libsodium-wrappers";
await sodium.ready;
export const fromHexString = (hexString: string) =>
    Uint8Array.from(
        hexString.match(/.{1,2}/g)!.map((byte) => parseInt(byte, 16))
    );

export const toHexString = (bytes: Uint8Array) =>
    bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, "0"), "");

export const toBase64 = (arr: Uint8Array) => {
    return sodium.to_base64(arr);
};
export const fromBase64 = (base64: string) => {
    return sodium.from_base64(base64);
};
