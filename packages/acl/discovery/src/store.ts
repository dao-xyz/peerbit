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
import { BinaryPayload } from "@dao-xyz/bpayload";
import { createDiscoveryStore, PeerInfo } from "./state";
import { RelationAccessController, TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';

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


@variant([0, 1])
export class DiscoveryStore extends ReadWriteAccessController<Operation<PeerInfo>> implements StoreLike<PeerInfo> {

    @field({ type: BinaryDocumentStore })
    info: BinaryDocumentStore<PeerInfo>

    @field({ type: RelationAccessController })
    networks: RelationAccessController;



    address: Address;

    constructor(props?: {
        name?: string,
        queryRegion?: string
    }) {
        super();
        if (props) {
            this.info = createDiscoveryStore(props);
        }
    }


    async canRead(_: any): Promise<boolean> {
        return true;
    }

    async canAppend(payload: MaybeEncrypted<Payload<Operation<PeerInfo>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
        // check if the peer id is trusted by the signature

        // i.e. load the network?
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