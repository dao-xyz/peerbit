import {
    PublicSignKey,
    Secp256k1PublicKey,
    Ed25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { Program, Address } from "@dao-xyz/peerbit-program";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { IPFS } from "ipfs-core-types";

export const parsePublicKey = (string: string): PublicSignKey | undefined => {
    const lc = string.toLowerCase();
    let parsed: PublicSignKey;
    if (lc.startsWith("ethereum/") || lc.startsWith("eth/")) {
        const address = lc.split("/")[1];
        parsed = new Secp256k1PublicKey({ address });
    } else if (
        lc.startsWith("ed25519/") ||
        lc.startsWith("solana/") ||
        lc.startsWith("sol/")
    ) {
        parsed = new Ed25519PublicKey({
            publicKey: Buffer.from(lc.split("/")[1], "hex"),
        });
    } else {
        return;
    }
    // verify address using ser/der (will fail if address lengths are wrong)
    try {
        deserialize(serialize(parsed), PublicSignKey);
    } catch (error) {
        return;
    }
    return parsed;
};

export const networkFromTopic = async (
    ipfs: IPFS,
    topic: string
): Promise<TrustedNetwork | undefined> => {
    const publicKey = parsePublicKey(topic);
    if (publicKey) {
        const network = new TrustedNetwork({ rootTrust: publicKey });
        return network;
    } else {
        try {
            const loaded = await Program.load<TrustedNetwork>(
                ipfs,
                Address.parse(topic)
            );
            if (loaded instanceof TrustedNetwork === false) {
                return undefined;
            }
        } catch (error) {
            return undefined;
        }
    }
};
