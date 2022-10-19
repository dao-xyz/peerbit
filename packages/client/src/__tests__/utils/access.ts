import { MaybeEncrypted, SignatureWithKey } from "@dao-xyz/peerbit-crypto"
import { Entry, Identity, Payload } from "@dao-xyz/ipfs-log"
import { IInitializationOptions, save } from "@dao-xyz/peerbit-dstore"
import { variant, field } from '@dao-xyz/borsh';
import { EventStore, Operation } from "./stores";
import { IPFS } from "ipfs-core-types";
import { Program } from "@dao-xyz/peerbit-program";


@variant([0, 251])
export class SimpleStoreContract extends Program {

    @field({ type: EventStore })
    store: EventStore<string>;

    constructor(properties?: { store: EventStore<string> }) {
        super();
        if (properties) {
            this.store = properties.store;
        }
    }

    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<any>): Promise<this> {
        const store = await options.saveOrResolve(ipfs, this);
        if (store !== this) {
            return store as this;
        }

        this.store = await this.store.init(ipfs, identity, options) as EventStore<string>
        await super.init(ipfs, identity, options)
        return this;
    }

    async save(ipfs: any, options?: { format?: string; pin?: boolean; timeout?: number; }) {
        const address = await save(ipfs, this, options)
        this.address = address;
        return address;
    }
}