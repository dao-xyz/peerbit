import { field, serialize, variant } from "@dao-xyz/borsh";
import {
    Documents,
    DocumentIndex,
    IndexedValue,
    DocumentQueryRequest,
    MemoryCompare,
    MemoryCompareQuery,
} from "@dao-xyz/peerbit-document";
import { PeerIdAddress, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { joinUint8Arrays } from "@dao-xyz/peerbit-borsh-utils";
import { RPC } from "@dao-xyz/peerbit-rpc";
import sodium from "libsodium-wrappers";
await sodium.ready;

abstract class KeyEnum {
    equals(other: KeyEnum): boolean {
        throw new Error("Not implemented");
    }

    hashCode(): string {
        throw new Error("Not implemented");
    }

    get key(): PublicSignKey | PeerIdAddress {
        throw new Error("Not implemented");
    }
}

@variant(0)
export class PK extends KeyEnum {
    @field({ type: PublicSignKey })
    _publicKey: PublicSignKey;

    constructor(properties: { publicKey: PublicSignKey } | PublicSignKey) {
        super();
        if (properties) {
            if (properties instanceof PublicSignKey) {
                this._publicKey = properties;
            } else {
                this._publicKey = properties.publicKey;
            }
        }
    }

    equals(other: KeyEnum): boolean {
        if (other instanceof PK) {
            return other.key.equals(this.key);
        }
        return false;
    }
    get key() {
        return this._publicKey;
    }
}

@variant(1)
export class IPFSId extends KeyEnum {
    @field({ type: PeerIdAddress })
    _id: PeerIdAddress;

    constructor(properties: { id: PeerIdAddress } | PeerIdAddress) {
        super();
        if (properties) {
            if (properties instanceof PeerIdAddress) {
                this._id = properties;
            } else {
                this._id = properties.id;
            }
        }
    }

    equals(other: KeyEnum): boolean {
        if (other instanceof IPFSId) {
            return other.key.equals(this.key);
        }
        return false;
    }

    get key() {
        return this._id;
    }
}

export type RelationResolver = {
    resolve: (
        key: PublicSignKey,
        db: Documents<IdentityRelation>
    ) => Promise<IdentityRelation[]>;
    next: (relation: IdentityRelation) => PublicSignKey;
};

export const OFFSET_TO_KEY = 73 + 1;
export const KEY_OFFSET = 7 + 43; // Relation discriminator + IdentityRelation discriminator + id length + utf8 encoding + id chars
export const getFromByTo: RelationResolver = {
    resolve: async (to: PublicSignKey, db: Documents<IdentityRelation>) => {
        const ser = serialize(to);
        return (
            await db.index.queryHandler(
                new DocumentQueryRequest({
                    queries: [
                        new MemoryCompareQuery({
                            compares: [
                                new MemoryCompare({
                                    bytes: ser,
                                    offset: BigInt(KEY_OFFSET + OFFSET_TO_KEY),
                                }),
                            ],
                        }),
                    ],
                })
            )
        ).map((x) => x.value);
    },
    next: (relation) => relation.from,
};

export const getToByFrom: RelationResolver = {
    resolve: async (from: PublicSignKey, db: Documents<IdentityRelation>) => {
        const ser = serialize(from);
        return (
            await db.index.queryHandler(
                new DocumentQueryRequest({
                    queries: [
                        new MemoryCompareQuery({
                            compares: [
                                new MemoryCompare({
                                    bytes: ser,
                                    offset: BigInt(KEY_OFFSET),
                                }),
                            ],
                        }),
                    ],
                })
            )
        ).map((x) => x.value);
    },
    next: (relation) => relation.to,
};

export async function* getPathGenerator(
    from: PublicSignKey | PeerIdAddress,
    db: Documents<IdentityRelation>,
    resolver: RelationResolver
) {
    let iter = [from];
    const visited = new Set();
    while (iter.length > 0) {
        const newIter: PublicSignKey[] = [];
        for (const value of iter) {
            const results = await resolver.resolve(value, db);
            for (const result of results) {
                if (result instanceof IdentityRelation) {
                    if (visited.has(result.id)) {
                        return;
                    }
                    visited.add(result.id);
                    yield result;

                    newIter.push(resolver.next(result));
                }
            }
        }
        iter = newIter;
    }
}

/**
 * Get path, to target.
 * @param start
 * @param target
 * @param db
 * @returns
 */
export const hasPathToTarget = async (
    start: PublicSignKey | PeerIdAddress,
    target: (key: PublicSignKey | PeerIdAddress) => boolean,
    db: Documents<IdentityRelation>,
    resolver: RelationResolver
): Promise<boolean> => {
    if (!db) {
        throw new Error("Not initalized");
    }

    const current = start;
    if (target(current)) {
        return true;
    }

    const iterator = getPathGenerator(current, db, resolver);
    for await (const relation of iterator) {
        if (target(relation.from)) {
            return true;
        }
    }
    return false;
};

@variant(0)
export abstract class AbstractRelation {
    @field({ type: "string" })
    id: string;
}

@variant(0)
export class IdentityRelation extends AbstractRelation {
    @field({ type: KeyEnum })
    _from: KeyEnum;

    @field({ type: Uint8Array })
    padding: Uint8Array;

    @field({ type: KeyEnum })
    _to: KeyEnum;

    constructor(properties?: {
        to: PublicSignKey | PeerIdAddress; // signed by truster
        from: PublicSignKey | PeerIdAddress;
    }) {
        super();
        if (properties) {
            this._from =
                properties.from instanceof PublicSignKey
                    ? new PK({ publicKey: properties.from })
                    : new IPFSId({ id: properties.from });
            this._to =
                properties.to instanceof PublicSignKey
                    ? new PK({ publicKey: properties.to })
                    : new IPFSId({ id: properties.to });
            const serFrom = serialize(this._from);
            this.padding = new Uint8Array(OFFSET_TO_KEY - serFrom.length - 4); // -4 comes from u32 describing length the padding array
            this.initializeId();
        }
    }

    get from(): PublicSignKey | PeerIdAddress {
        return this._from.key;
    }

    get to(): PublicSignKey | PeerIdAddress {
        return this._to.key;
    }

    initializeId() {
        this.id = IdentityRelation.id(this.to, this.from);
    }

    static id(to: PublicSignKey, from: PublicSignKey) {
        // we do sha1 to make sure id has fix length, this is important because we want the byte offest of the `trustee` and `truster` to be fixed
        return sodium.crypto_generichash(
            32,
            joinUint8Arrays([serialize(to), serialize(from)]),
            null,
            "base64"
        );
    }
}

export const hasPath = async (
    start: PublicSignKey | PeerIdAddress,
    end: PublicSignKey | PeerIdAddress,
    db: Documents<IdentityRelation>,
    resolver: RelationResolver
): Promise<boolean> => {
    return hasPathToTarget(start, (key) => end.equals(key), db, resolver);
};

export const getRelation = (
    from: PublicSignKey | PeerIdAddress,
    to: PublicSignKey | PeerIdAddress,
    db: Documents<IdentityRelation>
): IndexedValue<IdentityRelation> | undefined => {
    return db.index.get(new IdentityRelation({ from, to }).id);
};

export const createIdentityGraphStore = (props: {
    id: string;
    rpcRegion?: string;
}) =>
    new Documents<IdentityRelation>({
        index: new DocumentIndex({
            indexBy: "id",
            query: new RPC(),
        }),
    });
