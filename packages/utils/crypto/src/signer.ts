import { PublicSignKey } from "./key";

export interface Signer {
    sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicSignKey }>;
}