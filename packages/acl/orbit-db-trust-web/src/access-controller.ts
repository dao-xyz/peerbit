import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, Operation, PutOperation } from "@dao-xyz/orbit-db-bdocstore";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { Address, IInitializationOptions, load, save, StoreLike } from "@dao-xyz/orbit-db-store";
import { Entry, Payload } from "@dao-xyz/ipfs-log-entry";
import { createHash } from "crypto";
import { PublicKey } from "@dao-xyz/peerbit-crypto";
import isNode from 'is-node';
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { IPFS } from 'ipfs-core-types';
import { DeleteOperation } from "@dao-xyz/orbit-db-bdocstore";
import { ReadWriteAccessController } from "@dao-xyz/orbit-db-query-store";
import { Log } from "@dao-xyz/ipfs-log";
import Cache from '@dao-xyz/orbit-db-cache';
import EventEmitter from "events";
import { AnyRelation, createIdentityGraphStore, getPath, Relation } from "./identity-graph";
import { BinaryPayload } from "@dao-xyz/bpayload";

let v8 = undefined;
if (isNode) {
    v8 = require('v8');
}

@variant([0, 0])
export class AllowAllAccessController<T> extends ReadWriteAccessController<T>{


    async canRead(_): Promise<boolean> {
        return true;
    }

    async capAppend(_, __): Promise<boolean> {
        return true;
    }
}

const canAppendByRelation = async (payload: MaybeEncrypted<Payload<Operation<Relation>>>, keyEncrypted: MaybeEncrypted<PublicKey>, db: BinaryDocumentStore<Relation>, isTrusted?: (key: PublicKey) => Promise<boolean>): Promise<boolean> => {
    // verify the payload 
    const decrypted = (await payload.decrypt()).decrypted;
    const p = decrypted.getValue(Payload);
    const operation = p.init(db.encoding).value;

    if (operation instanceof PutOperation || operation instanceof DeleteOperation) {
        /*  const relation: Relation = operation.value || deserialize(operation.data, Relation); */
        await keyEncrypted.decrypt();
        const key = keyEncrypted.decrypted.getValue(PublicKey);

        if (operation instanceof PutOperation) {
            // TODO, this clause is only applicable when we modify the identityGraph, but it does not make sense that the canAppend method does not know what the payload will
            // be, upon deserialization. There should be known in the `canAppend` method whether we are appending to the identityGraph.

            const relation: BinaryPayload = operation._value || deserialize(operation.data, BinaryPayload);
            operation._value = relation;

            if (relation instanceof AnyRelation) {
                if (!relation.from.equals(key)) {
                    return false;
                }
            }

            // else assume the payload is accepted
        }

        if (isTrusted) {
            return isTrusted(key)
        }
        else {
            return true;
        }
    }

    else {
        return false;
    }
}

@variant([0, 1])
export class RelationAccessController extends ReadWriteAccessController<Relation> implements StoreLike<Relation> {

    @field({ type: BinaryDocumentStore })
    relationGraph: BinaryDocumentStore<Relation>

    address: Address;

    constructor(props?: {
        name?: string,
        queryRegion?: string
    }) {
        super();
        if (props) {
            this.relationGraph = createIdentityGraphStore(props);
        }
    }


    async canRead(_): Promise<boolean> {
        return true;
    }

    async canAppend(payload: MaybeEncrypted<Payload<Operation<Relation>>>, keyEncrypted: MaybeEncrypted<PublicKey>): Promise<boolean> {
        return canAppendByRelation(payload, keyEncrypted, this.relationGraph)
    }


    async init(ipfs: IPFS, key: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<any>): Promise<RelationAccessController> {
        const typeMap = options.typeMap ? { ...options.typeMap } : {}
        typeMap[Relation.name] = Relation;
        const saveOrResolved = await options.saveAndResolveStore(this);
        if (saveOrResolved !== this) {
            return saveOrResolved as RelationAccessController;
        }
        await this.relationGraph.init(ipfs, key, sign, { ...options, typeMap, fallbackAccessController: this }) // self referencing access controller
        return this;
    }


    async addRelation(to: PublicKey/*  | Identity | IdentitySerializable */) {
        /*  trustee = PublicKey.from(trustee); */
        await this.relationGraph.put(new AnyRelation({
            to: to,
            from: this.relationGraph.publicKey
        }));
    }

    sync(heads: Entry<any>[], leaderResolver: () => Promise<{ isLeader: boolean, leaders: string[] }>): Promise<void> {
        return this.relationGraph.sync(heads, leaderResolver);
    }
    async getHeads(): Promise<Entry<any>[]> {
        return this.relationGraph.getHeads();
    }

    get replicate(): boolean {
        return this.relationGraph.replicate;
    }

    get replicationTopic(): string {
        return this.relationGraph.replicationTopic;
    }

    drop?(): Promise<void> {
        return this.relationGraph.drop();
    }
    load?(): Promise<void> {
        return this.relationGraph.load();
    }
    get name(): string {
        return this.relationGraph.name;
    }

    async save(ipfs: any, options?: {
        format?: string;
        pin?: boolean;
        timeout?: number;
    }): Promise<Address> {
        const address = await save(ipfs, this, options)
        this.address = address;
        return address;
    }

    static load(ipfs: any, address: Address, options?: {
        timeout?: number;
    }) {
        return load(ipfs, address, RelationAccessController, options)
    }

    get id(): string {
        return this.relationGraph.id;
    }
    get oplog(): Log<any> {
        return this.relationGraph.oplog;
    }
    get cache(): Cache {
        return this.relationGraph.cache;
    }

    get events(): EventEmitter {
        return this.relationGraph.events;
    }

}


@variant([0, 2])
export class RegionAccessController extends ReadWriteAccessController<Relation> implements StoreLike<Relation> {

    @field({ type: PublicKey })
    rootTrust: PublicKey

    @field({ type: BinaryDocumentStore })
    trustGraph: BinaryDocumentStore<Relation>

    _orbitDB: OrbitDB;
    address: Address;

    constructor(props?: {
        name?: string,
        rootTrust: PublicKey
    }) {
        super();
        if (props) {
            this.trustGraph = createIdentityGraphStore(props);
            this.rootTrust = props.rootTrust;
        }
    }

    async init(ipfs: IPFS, key: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<any>): Promise<RegionAccessController> {
        const typeMap = options.typeMap ? { ...options.typeMap } : {}
        typeMap[Relation.name] = Relation;
        const saveOrResolved = await options.saveAndResolveStore(this);
        if (saveOrResolved !== this) {
            return saveOrResolved as RegionAccessController;
        }
        await this.trustGraph.init(ipfs, key, sign, { ...options, typeMap, fallbackAccessController: this }) // self referencing access controller
        return this;
    }


    async canAppend(payload: MaybeEncrypted<Payload<Operation<any>>>, keyEncrypted: MaybeEncrypted<PublicKey>): Promise<boolean> {

        // verify the payload 
        return canAppendByRelation(payload, keyEncrypted, this.trustGraph, async (key) => this.allowAll || await this.isTrusted(key))
    }

    async canRead(key: PublicKey): Promise<boolean> {

        if (this.allowAll) {
            return true;
        }

        const isTrusted = await this.isTrusted(key)
        return isTrusted;
    }

    async addTrust(trustee: PublicKey/*  | Identity | IdentitySerializable */) {
        /*  trustee = PublicKey.from(trustee); */
        await this.trustGraph.put(new AnyRelation({
            to: trustee,
            from: this.trustGraph.publicKey
        }));
    }



    /**
     * Follow trust path back to trust root.
     * Trust root is always trusted.
     * Hence if
     * Root trust A trust B trust C
     * C is trusted by Root
     * @param trustee 
     * @param truster, the truster "root", if undefined defaults to the root trust
     * @returns true, if trusted
     */
    async isTrusted(trustee: PublicKey, truster: PublicKey = this.rootTrust): Promise<boolean> {

        /*  trustee = PublicKey.from(trustee); */
        /**
         * TODO: Currently very inefficient
         */
        const trustPath = await getPath(trustee, truster, this.trustGraph);
        return !!trustPath
    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex')
    }

    sync(heads: Entry<any>[],): Promise<void> {
        return this.trustGraph.sync(heads);
    }
    async getHeads(): Promise<Entry<any>[]> {
        return this.trustGraph.getHeads();
    }

    get replicate(): boolean {
        return this.trustGraph.replicate;
    }

    get replicationTopic(): string {
        return this.trustGraph.replicationTopic;
    }

    drop?(): Promise<void> {
        return this.trustGraph.drop();
    }
    load?(): Promise<void> {
        return this.trustGraph.load();
    }
    get name(): string {
        return this.trustGraph.name;
    }

    async save(ipfs: any, options?: {
        format?: string;
        pin?: boolean;
        timeout?: number;
    }): Promise<Address> {
        const address = await save(ipfs, this, options)
        this.address = address;
        return address;
    }

    static load(ipfs: any, address: Address, options?: {
        timeout?: number;
    }) {
        return load(ipfs, address, RegionAccessController, options)
    }

    get id(): string {
        return this.trustGraph.id;
    }
    get oplog(): Log<any> {
        return this.trustGraph.oplog;
    }
    get cache(): Cache {
        return this.trustGraph.cache;
    }

    get events(): EventEmitter {
        return this.trustGraph.events;
    }
}

