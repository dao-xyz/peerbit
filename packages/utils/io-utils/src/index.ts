// TODO make imports dynamic/only load what is needed for the method below
import * as Block from "multiformats/block";
import { CID } from "multiformats/cid";
import * as dagPb from "@ipld/dag-pb";
import * as dagCbor from "@ipld/dag-cbor";
import * as raw from "multiformats/codecs/raw";
import { sha256 as hasher } from "multiformats/hashes/sha2";
import { base58btc } from "multiformats/bases/base58";
import { IPFS } from "ipfs-core-types";
import type { Message as PubSubMessage } from "@libp2p/interface-pubsub";
import { waitFor } from "@dao-xyz/peerbit-time";

const mhtype = "sha2-256";
const defaultBase = base58btc;
const unsupportedCodecError = () => new Error("unsupported codec");

const BLOCK_TRANSPORT_TOPIC = "_block";

const cidifyString = (str: string): CID => {
    if (!str) {
        return str as any as CID; // TODO fix types
    }

    return CID.parse(str);
};

const stringifyCid = (cid: any): string => {
    if (!cid || typeof cid === "string") {
        return cid;
    }

    const base = defaultBase;

    if (cid["/"]) {
        return cid["/"].toString(base);
    }

    return cid.toString(base);
};

const codecCodes = {
    [dagPb.code]: dagPb,
    [dagCbor.code]: dagCbor,
    [raw.code]: raw,
};
const codecMap = {
    raw: raw,
    "dag-pb": dagPb,
    "dag-cbor": dagCbor,
};

const _onMessage =
    (ipfs: IPFS, me: string) => async (message: PubSubMessage) => {
        // TODO dont hardcode timeouts
        if (message.type === "signed") {
            if (message.from.toString() === me) {
                return;
            }

            const decoder = new TextDecoder();

            try {
                const cid = cidifyString(
                    stringifyCid(decoder.decode(message.data))
                );
                const block = await ipfs.block.get(cid as any, {
                    timeout: 1000,
                });
                await ipfs.pubsub.publish(BLOCK_TRANSPORT_TOPIC, block);
            } catch (error) {
                return; // timeout o r invalid cid
            }
        }
    };

const announcePubSubBlocks = async (ipfs: IPFS) => {
    const topics = await ipfs.pubsub.ls();
    if (topics.indexOf(BLOCK_TRANSPORT_TOPIC) === -1) {
        await ipfs.pubsub.subscribe(
            BLOCK_TRANSPORT_TOPIC,
            _onMessage(ipfs, (await ipfs.id()).id.toString())
        );
    }
};

const getBlockValue = (block: Block.Block<unknown>, links?: string[]): any => {
    if (block.cid.code === dagPb.code) {
        return JSON.parse(new TextDecoder().decode((block.value as any).Data));
    }
    if (block.cid.code === dagCbor.code) {
        const value = block.value as any;
        links = links || [];
        links.forEach((prop) => {
            if (value[prop]) {
                value[prop] = Array.isArray(value[prop])
                    ? value[prop].map(stringifyCid)
                    : stringifyCid(value[prop]);
            }
        });
        return value;
    }
    if (block.cid.code === raw.code) {
        return block.value;
    }
    throw new Error("Unsupported");
};

async function readFromPubSub<T>(
    ipfs: IPFS,
    cid: string | CID,
    options: { timeout?: number; links?: string[] } = {}
): Promise<T | undefined> {
    const timeout = options.timeout || 5000;
    const cidString = stringifyCid(cid);
    const cidObject = cidifyString(cidString);
    const codec = (codecCodes as any)[cidObject.code];
    let value: T | undefined = undefined;
    const me = (await ipfs.id()).id.toString();
    const messageHandler = async (message: PubSubMessage) => {
        if (value) {
            return;
        }
        if (message.type === "signed") {
            if (message.from.toString() === me) {
                return;
            }

            const bytes = message.data;
            try {
                const decoded = await Block.decode({ bytes, codec, hasher });
                if (!cidObject.equals(decoded.cid)) {
                    return;
                }
                value = getBlockValue(decoded, options.links);
            } catch (error) {
                // invalid bytes like "CBOR decode error: not enough data for type"
                return;
            }
        }
    };
    await ipfs.pubsub.subscribe(BLOCK_TRANSPORT_TOPIC, messageHandler, {
        timeout,
    });
    await ipfs.pubsub.publish(
        BLOCK_TRANSPORT_TOPIC,
        new TextEncoder().encode(cidString)
    );

    try {
        await waitFor(() => value !== undefined, {
            timeout,
            delayInterval: 100,
        });
    } catch (error) {
        /// TODO, timeout or?
    } finally {
        await ipfs.pubsub.unsubscribe(BLOCK_TRANSPORT_TOPIC, messageHandler);
    }
    return value;
}

async function readFromBlock(
    ipfs: IPFS,
    cid: string | CID,
    options: { timeout?: number; links?: string[] } = {}
) {
    cid = cidifyString(stringifyCid(cid));
    const codec = (codecCodes as any)[cid.code];
    const bytes = await ipfs.block.get(cid as any, {
        timeout: options.timeout,
    });
    const block = await Block.decode({ bytes, codec, hasher });
    return getBlockValue(block, options?.links);
}

async function read(
    ipfs: IPFS,
    cid: string | CID,
    options: { timeout?: number; links?: string[] } = {}
) {
    const promises = [
        readFromPubSub(ipfs, cid, options),
        readFromBlock(ipfs, cid, options),
    ];
    const result = await Promise.any(promises);
    if (!result) {
        const results = await Promise.all(promises);
        for (const result of results) {
            if (result) {
                return result;
            }
        }
    }
    return result;
}

const prepareBlockWrite = (codec: any, value: any, links?: string[]) => {
    if (!codec) throw unsupportedCodecError();

    if (codec.code === dagPb.code) {
        value = typeof value === "string" ? value : JSON.stringify(value);
        value = { Data: new TextEncoder().encode(value), Links: [] };
    }
    if (codec.code === dagCbor.code) {
        links = links || [];
        links.forEach((prop) => {
            if (value[prop]) {
                value[prop] = Array.isArray(value[prop])
                    ? value[prop].map(cidifyString)
                    : cidifyString(value[prop]);
            }
        });
    }
    return value;
};

async function write(
    ipfs: IPFS,
    format: string,
    value: any,
    options: {
        base?: any;
        pin?: boolean;
        timeout?: number;
        links?: string[];
        onlyHash?: boolean;
    } = {}
) {
    const codec = (codecMap as any)[format];
    value = prepareBlockWrite(codec, value, options?.links);
    const block = await Block.encode({ value, codec, hasher });

    if (!options.onlyHash) {
        await ipfs.block.put(block.bytes, {
            /* cid: block.cid.bytes, */
            version: block.cid.version,
            format,
            mhtype,
            pin: options.pin,
            timeout: options.timeout,
        });
    }

    const cid = codec.code === dagPb.code ? block.cid.toV0() : block.cid;
    const cidString = cid.toString(options.base || defaultBase);

    await announcePubSubBlocks(ipfs);

    return cidString;
}

async function rm(ipfs: IPFS, hash: CID | string) {
    try {
        await ipfs.pin.rm(hash as any);
    } catch (error) {
        // not pinned // TODO add bettor error handling
    }
    for await (const result of ipfs.block.rm(
        (hash instanceof CID ? hash : CID.parse(hash)) as any
    )) {
        if (result.error) {
            throw new Error(
                `Failed to remove block ${result.cid} due to ${result.error.message}`
            );
        }
    }
}

export default {
    read,
    write,
    readFromBlock,
    readFromPubSub,
    rm,
    BLOCK_TRANSPORT_TOPIC,
};
