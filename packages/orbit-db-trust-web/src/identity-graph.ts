import { field, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore } from "@dao-xyz/orbit-db-bdocstore";
import { PublicKey } from "@dao-xyz/identity";
import isNode from 'is-node';
import { SystemBinaryPayload } from "@dao-xyz/bpayload";
import { DocumentQueryRequest, MemoryCompare, MemoryCompareQuery, QueryRequestV0, Result, ResultWithSource } from "@dao-xyz/query-protocol";
import { AccessController } from "@dao-xyz/orbit-db-store";
import { createHash } from "crypto";

let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}

export const getFromByTo = async (to: PublicKey, db: BinaryDocumentStore<Relation>) => {
    const ser = serialize(to);
    return await db.queryHandler(new QueryRequestV0({
        type: new DocumentQueryRequest({
            queries: [
                new MemoryCompareQuery({
                    compares: [
                        new MemoryCompare({
                            bytes: ser,
                            offset: 3n + 4n + 1n + 28n // SystemBinaryPayload discriminator + Relation discriminator + AnyRelation discriminator + id length u32 + utf8 encoding + id chars 
                        })
                    ]
                })
            ]
        })
    }));
}


export async function* getFromByToGenerator(from: PublicKey, db: BinaryDocumentStore<Relation>) {
    let iter = [from];
    const visited = new Set();
    while (iter.length > 0) {
        const newIter = [];
        for (const value of iter) {
            const results = await getFromByTo(value, db);
            for (const result of results) {
                if (result instanceof ResultWithSource) {
                    if (result.source instanceof AnyRelation) {
                        if (visited.has(result.source.id)) {
                            return;
                        }
                        visited.add(result.source.id);
                        yield result.source;

                        newIter.push(result.source.from);
                    }
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
export const getTargetPath = async (start: PublicKey, target: (key: PublicKey) => boolean, db: BinaryDocumentStore<Relation>): Promise<Relation[]> => {

    if (!db) {
        throw new Error("Not initalized")
    }

    let path: Relation[] = [];
    let current = start;
    if (target(current)) {
        return path;
    }

    const iterator = getFromByToGenerator(current, db);
    for await (const relation of iterator) {
        if (target(relation.from)) {
            return path;
        }
    }
    return undefined;
}


@variant(1)
export class Relation extends SystemBinaryPayload {

    @field({ type: 'string' })
    id: string;

}

// Any key provider -> Orbitdb
@variant(0)
export class AnyRelation extends Relation {

    @field({ type: PublicKey })
    to: PublicKey

    @field({ type: PublicKey })
    from: PublicKey

    constructor(properties?: {
        to: PublicKey // signed by truster
        from: PublicKey
    }) {
        super();
        if (properties) {
            this.to = properties.to;
            this.from = properties.from;
            /*             this.signature = properties.signature;
             */
            this.initializeId();
        }
    }

    initializeId() {
        this.id = AnyRelation.id(this.to, this.from);
    }

    static id(to: PublicKey, from: PublicKey) {
        // we do sha1 to make sure id has fix length, this is important because we want the byte offest of the `trustee` and `truster` to be fixed
        return createHash('sha1').update(new Uint8Array(Buffer.concat([serialize(to), serialize(from)]))).digest('base64');
    }

}


export const getPath = async (start: PublicKey, end: PublicKey, db: BinaryDocumentStore<Relation>): Promise<Relation[]> => {
    return getTargetPath(start, (key) => end.equals(key), db)
}



export const createIdentityGraphStore = (props: { name?: string, queryRegion?: string, accessController?: AccessController<any> }) => new BinaryDocumentStore<Relation>({
    indexBy: 'id',
    name: props?.name ? props?.name : '' + '_relation',
    accessController: undefined,
    objectType: Relation.name,
    queryRegion: props.queryRegion,
    clazz: Relation
})