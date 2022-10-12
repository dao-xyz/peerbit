import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, Operation, PutOperation } from "@dao-xyz/orbit-db-bdocstore";
import { OrbitDB } from "@dao-xyz/orbit-db";
import { Address, IInitializationOptions, load, save, StoreLike } from "@dao-xyz/orbit-db-store";
import { BORSH_ENCODING, Entry, Identity, Payload } from "@dao-xyz/ipfs-log";
import { createHash } from "crypto";
import { IPFSAddress, Key, OtherKey, PublicSignKey, SignatureWithKey } from "@dao-xyz/peerbit-crypto";

import type { PeerId } from '@libp2p/interface-peer-id';
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { IPFS } from 'ipfs-core-types';
import { DeleteOperation } from "@dao-xyz/orbit-db-bdocstore";
import { ReadWriteAccessController } from "@dao-xyz/orbit-db-query-store";
import { Log } from "@dao-xyz/ipfs-log";
import { AnyRelation, createIdentityGraphStore, getPathGenerator, getPath, Relation, getFromByTo, getToByFrom, hasRelation } from "./identity-graph";
import { BinaryPayload } from "@dao-xyz/bpayload";


@variant([0, 0])
export class AllowAllAccessController<T> extends ReadWriteAccessController<T>{


    async canRead(_: any): Promise<boolean> {
        return true;
    }

    async capAppend(_: any, __: any): Promise<boolean> {
        return true;
    }
}
const encoding = BORSH_ENCODING(Operation);
const canAppendByRelation = async (mpayload: MaybeEncrypted<Payload<Operation<any>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>, db: BinaryDocumentStore<Relation>, isTrusted?: (key: PublicSignKey) => Promise<boolean>): Promise<boolean> => {
    // verify the payload 
    const decrypted = (await mpayload.decrypt(db.oplog._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)))).decrypted;
    const payload = decrypted.getValue(Payload);
    const operation = payload.getValue(encoding);
    if (operation instanceof PutOperation || operation instanceof DeleteOperation) {
        /*  const relation: Relation = operation.value || deserialize(operation.data, Relation); */
        await keyEncrypted.decrypt(db.oplog._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)));
        const key = keyEncrypted.decrypted.getValue(SignatureWithKey).publicKey;

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
            const trusted = await isTrusted(key);
            return trusted
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
export class RelationAccessController extends ReadWriteAccessController<Operation<Relation>> implements StoreLike<Relation> {

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


    async canRead(_: any): Promise<boolean> {
        return true;
    }

    async canAppend(payload: MaybeEncrypted<Payload<Operation<Relation>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
        return canAppendByRelation(payload, keyEncrypted, this.relationGraph)
    }


    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<Operation<Relation>>): Promise<RelationAccessController> {
        const typeMap = options.typeMap ? { ...options.typeMap } : {}
        typeMap[Relation.name] = Relation;
        const saveOrResolved = await options.saveAndResolveStore(ipfs, this);
        if (saveOrResolved !== this) {
            return saveOrResolved as RelationAccessController;
        }
        await this.relationGraph.init(ipfs, identity, { ...options, typeMap, fallbackAccessController: this }) // self referencing access controller
        return this;
    }


    async addRelation(to: PublicSignKey/*  | Identity | IdentitySerializable */) {
        /*  trustee = PublicKey.from(trustee); */
        await this.relationGraph.put(new AnyRelation({
            to: to,
            from: this.relationGraph.identity.publicKey
        }));
    }

    sync(heads: Entry<any>[]): Promise<void> {
        return this.relationGraph.sync(heads);
    }

    get replicate(): boolean {
        return this.relationGraph.replicate;
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

}


@variant([0, 2])
export class TrustedNetwork extends ReadWriteAccessController<Operation<Relation>> implements StoreLike<Relation> {

    @field({ type: PublicSignKey })
    rootTrust: PublicSignKey

    @field({ type: BinaryDocumentStore })
    trustGraph: BinaryDocumentStore<Relation>

    _orbitDB: OrbitDB;
    address: Address;

    constructor(props?: {
        name?: string,
        rootTrust: PublicSignKey
    }) {
        super();
        if (props) {
            this.trustGraph = createIdentityGraphStore(props);
            this.rootTrust = props.rootTrust;
        }
    }

    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<TrustedNetwork> {
        const typeMap = options.typeMap ? { ...options.typeMap } : {}
        typeMap[Relation.name] = Relation;
        const saveOrResolved = await options.saveAndResolveStore(ipfs, this);
        if (saveOrResolved !== this) {
            return saveOrResolved as TrustedNetwork;
        }
        await this.trustGraph.init(ipfs, identity, { ...options, typeMap, fallbackAccessController: this }) // self referencing access controller
        this.address = saveOrResolved.address;
        return this;
    }


    async canAppend(payload: MaybeEncrypted<Payload<Operation<any>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {

        // verify the payload 
        return canAppendByRelation(payload, keyEncrypted, this.trustGraph, async (key) => this.allowAll || await this.isTrusted(key))
    }

    async canRead(key: PublicSignKey): Promise<boolean> {

        if (this.allowAll) {
            return true;
        }

        const isTrusted = await this.isTrusted(key)
        return isTrusted;
    }

    async add(trustee: PublicSignKey | PeerId/*  | Identity | IdentitySerializable */) {
        /*  trustee = PublicKey.from(trustee); */
        if (!this.hasRelation(trustee, this.trustGraph.identity.publicKey)) {
            await this.trustGraph.put(new AnyRelation({
                to: trustee instanceof Key ? trustee : new IPFSAddress({ address: trustee.toString() }),
                from: this.trustGraph.identity.publicKey
            }));
        }
    }

    hasRelation(trustee: PublicSignKey | PeerId, truster = this.rootTrust) {
        return !!hasRelation(truster, trustee instanceof Key ? trustee : new IPFSAddress({ address: trustee.toString() }), this.trustGraph)[0]?.value;
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
    async isTrusted(trustee: PublicSignKey | OtherKey, truster: PublicSignKey = this.rootTrust): Promise<boolean> {

        /*  trustee = PublicKey.from(trustee); */
        /**
         * TODO: Currently very inefficient
         */
        const trustPath = await getPath(trustee, truster, this.trustGraph, getFromByTo);
        return !!trustPath
    }


    async getTrusted(): Promise<PublicSignKey[]> {
        let current = this.rootTrust;
        const participants: PublicSignKey[] = [current];
        let generator = getPathGenerator(current, this.trustGraph, getToByFrom);
        for await (const next of generator) {
            participants.push(next.to);
        }
        return participants;

    }


    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex')
    }

    sync(heads: Entry<any>[],): Promise<void> {
        return this.trustGraph.sync(heads);
    }


    get replicate(): boolean {
        return this.trustGraph.replicate;
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
        return load(ipfs, address, TrustedNetwork, options)
    }

    get id(): string {
        return this.trustGraph.id;
    }
    get oplog(): Log<any> {
        return this.trustGraph.oplog;
    }
}

