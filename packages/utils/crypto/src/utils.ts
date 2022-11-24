import isNode from "is-node";

export const fromHexString = (hexString: string) =>
    Uint8Array.from(
        hexString.match(/.{1,2}/g)!.map((byte) => parseInt(byte, 16))
    );

export const toHexString = (bytes: Uint8Array) =>
    bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, "0"), "");

export const toBase64 = (arr: Uint8Array) =>
    isNode
        ? Buffer.from(arr).toString("base64")
        : window.btoa(String.fromCharCode(...arr));
export const fromBase64 = (base64: string) =>
    isNode
        ? new Uint8Array(Buffer.from(base64, "base64"))
        : Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
