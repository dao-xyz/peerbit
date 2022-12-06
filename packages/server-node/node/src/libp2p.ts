import { createLibp2p } from "libp2p";
import { noise } from "@chainsafe/libp2p-noise";
import { mplex } from "@libp2p/mplex";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import { peerIdFromKeys } from "@libp2p/peer-id";
import { Ed25519Keypair, fromBase64, toBase64 } from "@dao-xyz/peerbit-crypto";
import { supportedKeys } from "@libp2p/crypto/keys";
import { getConfigDir, getKeysPath, NotFoundError } from "./config.js";
import { checkExistPath } from "./api.js";
import { waitFor } from "@dao-xyz/peerbit-time";
import { serialize, deserialize } from "@dao-xyz/borsh";

export const saveKeys = async (keypair: Ed25519Keypair): Promise<void> => {
    const fs = await import("fs");
    const configDir = await getConfigDir();
    const keysPath = await getKeysPath(configDir);
    if (await checkExistPath(keysPath)) {
        throw new Error(
            "Config path for keys: " + keysPath + ", already exist"
        );
    }
    console.log(`Creating config folder ${configDir}`);

    fs.mkdirSync(configDir, { recursive: true });
    await waitFor(() => fs.existsSync(configDir));

    console.log(`Created config folder ${configDir}`);

    const serialized = serialize(keypair);
    fs.writeFileSync(
        keysPath,
        JSON.stringify({ keypair: toBase64(serialized) })
    );
    console.log(`Created keys at ${keysPath}`);
};

export const loadKeys = async (): Promise<Ed25519Keypair> => {
    const fs = await import("fs");
    const configDir = await getConfigDir();
    const keysPath = await getKeysPath(configDir);
    if (!(await checkExistPath(keysPath))) {
        throw new NotFoundError("Keys file does not exist");
    }
    const keypairBase64 = JSON.parse(
        fs.readFileSync(keysPath, "utf-8")
    )?.keypair;
    if (!keypairBase64 || keypairBase64.length === 0) {
        throw new NotFoundError("Keypair not found");
    }
    return deserialize(fromBase64(keypairBase64), Ed25519Keypair);
};

export const createNode = async () => {
    let keypair: Ed25519Keypair;
    try {
        keypair = await loadKeys();
    } catch (error) {
        keypair = Ed25519Keypair.create();
        await saveKeys(keypair);
    }
    const peerId = await peerIdFromKeys(
        new supportedKeys["ed25519"].Ed25519PublicKey(
            keypair.publicKey.publicKey
        ).bytes,
        new supportedKeys["ed25519"].Ed25519PrivateKey(
            keypair.privateKey.privateKey,
            keypair.publicKey.publicKey
        ).bytes
    ); // marshalPublicKey({ bytes: keypair.publicKey.bytes }, 'ed25519'), marshalPrivateKey({ bytes: keypair.privateKey.bytes }, 'ed25519')
    const node = await createLibp2p({
        peerId,
        connectionManager: {
            autoDial: true,
        },
        addresses: {
            listen: ["/ip4/127.0.0.1/tcp/8001", "/ip4/127.0.0.1/tcp/8002/ws"],
        },
        transports: [tcp(), webSockets()],
        connectionEncryption: [noise()],
        streamMuxers: [mplex()],
        pubsub: gossipsub(),
    });
    await node.start();
    return node;
};
