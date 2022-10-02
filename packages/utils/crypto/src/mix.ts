import { Constructor, deserialize } from "@dao-xyz/borsh";
import { SignKey } from "./key";
import { MaybeSigned } from "./signature";
import { MaybeEncrypted, PublicKeyEncryption } from "./x25519";
import { AccessError } from './errors.js'
export const decryptVerifyInto = async <T>(data: Uint8Array, clazz: Constructor<T>, encryption?: PublicKeyEncryption, options: { isTrusted?: (key: SignKey) => Promise<boolean> } = {}) => {
    const maybeEncrypted = deserialize<MaybeEncrypted<MaybeSigned<any>>>(Buffer.from(data), MaybeEncrypted);
    const decrypted = await (encryption ? maybeEncrypted.init(encryption) : maybeEncrypted).decrypt();
    const maybeSigned = decrypted.getValue(MaybeSigned);
    if (!await maybeSigned.verify()) {
        throw new AccessError();
    }

    if (options.isTrusted) {
        if (!maybeSigned.signature) {
            throw new AccessError();
        }

        if (!await options.isTrusted(maybeSigned.signature.publicKey)) {
            throw new AccessError();
        }
    }
    return deserialize(maybeSigned.data, clazz);
}