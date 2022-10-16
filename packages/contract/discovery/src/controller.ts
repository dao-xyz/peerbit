import { deserialize, field, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore, Operation, PutOperation } from "@dao-xyz/peerbit-ddoc";
import { Address, IInitializationOptions } from "@dao-xyz/peerbit-dstore";
import { BORSH_ENCODING, Identity, Payload } from "@dao-xyz/ipfs-log";
import { PublicSignKey, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { IPFS } from 'ipfs-core-types';
import { createDiscoveryStore, NetworInfo } from "./state";
import { AnyRelation, Relation, RelationContract, TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';
import { Contract } from "@dao-xyz/peerbit-contract";
import { multiaddr } from '@multiformats/multiaddr';

const encoding = BORSH_ENCODING(Operation);
@variant([0, 1])
export class TrustedBootstrap extends Contract {

    @field({ type: BinaryDocumentStore })
    info: BinaryDocumentStore<NetworInfo>

    @field({ type: RelationContract })
    relations: RelationContract;

    _peerId: string;

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

    async canAppend(mpayload: MaybeEncrypted<Payload<Operation<NetworInfo>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
        // check if the peer id is trusted by the signature
        const decrypted = (await mpayload.decrypt(this.relations.relationGraph.oplog._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)))).decrypted;
        const payload = decrypted.getValue(Payload);
        const operation = payload.getValue(encoding);

        // i.e. load the network?
        if (operation instanceof PutOperation) {

            const info: NetworInfo = operation._value || deserialize(operation.data, NetworInfo);
            const existingAddresses = await this.info._ipfs.swarm.peers();
            const existingAddressesSet = new Set(existingAddresses.map(x => x.addr.toString()));
            for (const a of info.addresses) {
                if (existingAddressesSet.has(a)) {
                    continue;
                }
                if (info.id === this._peerId) {
                    continue;
                }
                const suffix = '/p2p/' + info.id;
                await this.info._ipfs.swarm.connect(multiaddr(a.toString() + (a.indexOf(suffix) === -1 ? suffix : '')));
            }

            const network: TrustedNetwork = await Contract.load(this.info._ipfs, info.network)
            network.query(new Queryre)

        }

        return false;
    }


    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<this> {
        this._peerId = (await ipfs.id()).id.toString();
        const saveOrResolved = await options.saveOrResolve(ipfs, this);
        if (saveOrResolved !== this) {
            return saveOrResolved as this;
        }
        await this.relations.init(ipfs, identity, { ...options, canRead: this.canRead.bind(this), canAppend: this.canAppend.bind(this) }) // self referencing access controller
        return this;
    }


    async addRelation(to: PublicSignKey/*  | Identity | IdentitySerializable */) {
        /*  trustee = PublicKey.from(trustee); */
        await this.relations.relationGraph.put(new AnyRelation({
            to: to,
            from: this.relations.relationGraph.oplog._identity.publicKey
        }));
    }

}