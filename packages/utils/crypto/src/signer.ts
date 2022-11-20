import { PublicSignKey } from "./key";

export interface Signer {
    sign: (bytes: Uint8Array) => Promise<Uint8Array>;
}

export type SignWithKey = (
    bytes: Uint8Array
) => Promise<{ signature: Uint8Array; publicKey: PublicSignKey }>;
