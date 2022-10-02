import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto"
import { Entry, Payload } from "@dao-xyz/ipfs-log-entry"
import { AccessController, Address, IInitializationOptions, save, Store, StoreLike } from "@dao-xyz/orbit-db-store"
import { variant, field } from '@dao-xyz/borsh';
import { Ed25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { Log } from "@dao-xyz/ipfs-log";
import Cache from '@dao-xyz/orbit-db-cache';
import { EventStore, Operation } from "./stores";
import EventEmitter from "events";
@variant([0, 253])
export class SimpleAccessController<T> extends AccessController<T>
{
    async canAppend(payload: MaybeEncrypted<Payload<T>>, signKey: MaybeEncrypted<Ed25519PublicKey>) {
        return true;
    }
}

@variant([0, 252])
export class SimpleStoreAccessController extends AccessController<any> implements StoreLike<Operation<string>>
{
    @field({ type: EventStore })
    store: EventStore<string>;

    address: Address;
    _options: IInitializationOptions<any>
    constructor(properties?: { store: EventStore<string> }) {
        super();
        if (properties) {
            this.store = properties.store;
        }
    }

    async canAppend(payload: MaybeEncrypted<Payload<any>>, signKey: MaybeEncrypted<Ed25519PublicKey>) {
        return true;
    }

    drop(): Promise<void> {
        return this.store.drop()
    }

    load(): Promise<void> {
        return this.store.load()
    }
    async init(ipfs, key, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<any>): Promise<SimpleStoreAccessController> {

        options.fallbackAccessController = this;
        this._options = options;
        const store = await options.saveAndResolveStore(this);
        if (store !== this) {
            return store as SimpleStoreAccessController;
        }

        this.store = await this.store.init(ipfs, key, sign, options) as EventStore<string>
        return this;
    }

    async save(ipfs: any, options?: { format?: string; pin?: boolean; timeout?: number; }) {
        const address = await save(ipfs, this, options)
        this.address = address;
        return address;
    }

    sync(heads: Entry<Operation<string>>[]): Promise<void> {
        return this.store.sync(heads)
    }
    get replicationTopic(): string {
        return Store.getReplicationTopic(this.address, this._options);
    }
    get events(): EventEmitter {
        return this.store.events
    }

    /*   get allowForks(): boolean {
          return this.store.allowForks;
      } */

    get oplog(): Log<Operation<string>> {
        return this.store.oplog;
    }
    get cache(): Cache {
        return this.store.cache
    }
    get id(): string {
        return this.store.id;
    }
    get replicate(): boolean {
        return this.store.replicate;
    }

    getHeads(): Promise<Entry<Operation<string>>[]> {
        return this.store.getHeads();
    }
    get name(): string {
        return this.store.name;
    }

}