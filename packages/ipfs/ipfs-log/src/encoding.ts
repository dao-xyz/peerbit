import { Constructor, deserialize, serialize } from "@dao-xyz/borsh";
import stringify from "json-stringify-deterministic";

export interface Encoding<T> {
    encoder: (data: T) => Uint8Array;
    decoder: (bytes: Uint8Array) => T;
}
export const JSON_ENCODING: Encoding<any> = {
    encoder: (obj: any) => {
        return new Uint8Array(Buffer.from(stringify(obj)));
    },
    decoder: (bytes: Uint8Array) => {
        return JSON.parse(Buffer.from(bytes).toString());
    },
};

export const BORSH_ENCODING = <T>(clazz: Constructor<T>): Encoding<T> => {
    return {
        decoder: (bytes: Uint8Array) => deserialize(bytes, clazz),
        encoder: (data: any) => serialize(data),
    };
};
