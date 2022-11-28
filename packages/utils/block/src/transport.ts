// TODO make imports dynamic/only load what is needed for the method below
import * as Block from "multiformats/block";
import { CID } from "multiformats/cid";
import * as dagPb from "@ipld/dag-pb";
import * as dagCbor from "@ipld/dag-cbor";
import * as raw from "multiformats/codecs/raw";
import { sha256 as hasher } from "multiformats/hashes/sha2";
import { base58btc } from "multiformats/bases/base58";
import { waitFor } from "@dao-xyz/peerbit-time";
import { Libp2p } from 'libp2p'

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
    libp2p: Libp2p,
    cid: string | CID,
    options: { timeout?: number; links?: string[] } = {}
): Promise<T | undefined> {
    const timeout = options.timeout || 5000;
    const cidString = stringifyCid(cid);
    const cidObject = cidifyString(cidString);
    const codec = (codecCodes as any)[cidObject.code];
    let value: T | undefined = undefined;
    // await libp2p.pubsub.subscribe(BLOCK_TRANSPORT_TOPIC) TODO
    const eventHandler = async (evt) => {
        if (value) {
            return;
        }
        const message = evt.detail;
        if (message.type === "signed") {
            if (message.from.equals(libp2p.peerId)) {
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
    }
    libp2p.pubsub.addEventListener('message', eventHandler);
    await libp2p.pubsub.publish(
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
        await libp2p.pubsub.removeEventListener('message', eventHandler);
    }
    return value;
}


async function read(
    libp2p: Libp2p,
    cid: string | CID,
    options: { timeout?: number; links?: string[] } = {}
) {
    const promises = [
        readFromPubSub(libp2p, cid, options),
        /* readFromBlock(ipfs, cid, options), */
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
    saveBlock: (block: Block.Block<any>) => Promise<void> | void,
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
        await saveBlock(block);
    }

    const cid = codec.code === dagPb.code ? block.cid.toV0() : block.cid;
    const cidString = cid.toString(options.base || defaultBase);
    return cidString;
}

export default {
    read,
    write,
    readFromPubSub,
    BLOCK_TRANSPORT_TOPIC,
};
