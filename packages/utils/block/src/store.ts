import { AbstractLevel } from 'abstract-level';
import { CID } from "multiformats/cid";
import * as Block from "multiformats/block";
import * as dagPb from "@ipld/dag-pb";
import * as dagCbor from "@ipld/dag-cbor";
import * as raw from "multiformats/codecs/raw";
import { base58btc } from "multiformats/bases/base58";
import { sha256 as hasher } from "multiformats/hashes/sha2";
import { Libp2p } from 'libp2p';
import transport from './transport.js';

const defaultBase = base58btc;
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


const getBlockValue = async <T>(block: Block.Block<unknown>, links?: string[]): Promise<T> => {
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
        return block.value as T;
    }
    throw new Error("Unsupported");
};

export interface BlockStore {
    get<T>(cid: CID | string, codec: any): Promise<T>;
    close(): Promise<void>
}

export class LevelBlockStore implements BlockStore {

    _level: AbstractLevel<any, string, Uint8Array>

    constructor(level: AbstractLevel<any, string, Uint8Array>) {
        this._level = level;
    }

    async get<T>(cid: CID | string, codec: any = raw): Promise<T> {
        const str = cid.toString();
        const bytes = await this._level.get(str);
        const block = await Block.decode({ bytes, codec, hasher })
        return getBlockValue(block);
    }

    close(): Promise<void> {
        return this._level.close();
    }
}

export class LibP2PBlockStore implements BlockStore {
    _store: LevelBlockStore
    _libp2p: Libp2p

    constructor(libp2p: Libp2p, store: LevelBlockStore) {
        this._store = store;
        this._libp2p = libp2p;
    }

    async get<T>(cid: CID | string, codec: any = raw): Promise<T> {

        const locally = await this._store.get(cid);
        if (locally) {
            return locally as T;
        }

        transport.readqwdasa

        // try to get it remotelly
    }

    close(): Promise<void> {
        return this._level.close();
    }

} 