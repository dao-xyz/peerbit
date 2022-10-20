import { deserialize, field, variant } from "@dao-xyz/borsh";
import { DDocs, DeleteOperation, Operation, PutOperation } from "@dao-xyz/peerbit-ddoc";
import { Address, IInitializationOptions } from "@dao-xyz/peerbit-dstore";
import { BORSH_ENCODING, Identity, Payload } from "@dao-xyz/ipfs-log";
import { PublicSignKey, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { IPFS } from 'ipfs-core-types';
import { createDiscoveryStore, NetworkInfo } from "./state";
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';
import { Program, ProgramInitializationOptions } from "@dao-xyz/peerbit-program";
import { multiaddr } from '@multiformats/multiaddr';

const encoding = BORSH_ENCODING(Operation);

@variant([0, 20])
export class NetworkDiscovery extends Program {

    @field({ type: DDocs })
    info: DDocs<NetworkInfo>

    _peerId: string;
    _ipfs: IPFS;
    _identity: Identity;
    _options: ProgramInitializationOptions;

    constructor(props?: {
        name?: string,
        queryRegion?: string
    }) {
        super(props);
        this.info = createDiscoveryStore(props);

    }

    async canAppend(mpayload: MaybeEncrypted<Payload<Operation<NetworkInfo>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
        // check if the peer id is trusted by the signature
        const kr = this.info.store.oplog._encryption?.getAnyKeypair || (() => Promise.resolve(undefined));
        const decrypted = (await mpayload.decrypt(kr)).decrypted;
        const payload = decrypted.getValue(Payload);
        const operation = payload.getValue(encoding);

        // i.e. load the network?
        if (operation instanceof PutOperation || operation instanceof DeleteOperation) {

            let info: NetworkInfo;
            if (operation instanceof DeleteOperation) {
                const infos = this.info.get(operation.key)
                if (infos.length === 0 || infos.length > 1) {
                    return false;
                }
                info = infos[0].value

            }
            else {
                info = operation._value || deserialize(operation.data, NetworkInfo)
            }
            const existingAddresses = await this.info.store._ipfs.swarm.peers();
            const existingAddressesSet = new Set(existingAddresses.map(x => x.addr.toString()));

            const suffix = '/p2p/' + info.peerId;
            const getMAddress = (a: string) => multiaddr(a.toString() + (a.indexOf(suffix) === -1 ? suffix : ''))

            const isNotMe = info.peerId !== this._peerId;
            if (isNotMe) {
                await Promise.all(info.addresses.filter((a) => !existingAddressesSet.has(a)).map((a) => this._ipfs.swarm.connect(getMAddress(a))))
            }
            const network: TrustedNetwork = await Program.load(this.info.store._ipfs, Address.parse(info.network))

            await network.init(this._ipfs, this._identity, { ...this._options, store: { ...this._options.store, replicate: false } })
            let isTrusted: boolean = await network.isTrusted((await keyEncrypted.decrypt(kr)).getValue(SignatureWithKey).publicKey)


            // Close open connections
            if (isNotMe) {
                await Promise.all(info.addresses.filter((a) => !existingAddressesSet.has(a)).map((a) => this._ipfs.swarm.disconnect(getMAddress(a))))
            }
            return isTrusted
        }

        return false;
    }


    async init(ipfs: IPFS, identity: Identity, options: ProgramInitializationOptions): Promise<this> {
        this._peerId = (await ipfs.id()).id.toString();
        this._ipfs = ipfs;
        this._identity = identity;
        this._options = options;
        await this.info.init(ipfs, identity, { ...options, store: { ...options.store, typeMap: { [NetworkInfo.name]: NetworkInfo }, canAppend: this.canAppend.bind(this) } }) // self referencing access controller
        await super.init(ipfs, identity, options);
        return this;
    }


    async addInfo(network: TrustedNetwork) {
        const id = await this._ipfs.id();
        const isNotLocalhostAddress = (addr: string) => !addr.toString().includes('/127.0.0.1/')
        return this.info.put(new NetworkInfo({
            network: network.address,
            peerId: id.id.toString(),
            addresses: id.addresses.map(x => x.toString()).filter(isNotLocalhostAddress)
        }))
    }

}