// TODO extend IPFS-LOG access controller interface for canAppend method
import { variant, field } from '@dao-xyz/borsh';
import { IInitializationOptions, IStoreOptions, Store } from './store';
import { AccessController } from './access-controller';
import { StoreLike } from './store-like';
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { Address, load, save } from './io';
import EventEmitter from 'events';
import { Log } from '@dao-xyz/ipfs-log';
import Cache from '@dao-xyz/orbit-db-cache';
import { Entry } from '@dao-xyz/ipfs-log-entry';

@variant([0, 1])
export class StoreAccessController<A, S extends StoreLike<any>, T> extends AccessController<T> implements StoreLike<A>  {

    @field({ type: Store })
    store: S

    address: Address;

    constructor(properties?: { store: S }) {
        super();
        if (properties) {
            this.store = properties.store;
        }
    }
    sync(heads: Entry<any>[]): Promise<void> {
        return this.store.sync(heads);
    }
    async getHeads(): Promise<Entry<any>[]> {
        return this.store.getHeads();
    }

    get replicate(): boolean {
        return this.store.replicate;
    }

    get replicationTopic(): string {
        return this.store.replicationTopic;
    }

    async init(ipfs: any, identity: Identity, options: IInitializationOptions<any>): Promise<void> {
        await this.save(ipfs);
        return this.store.init(ipfs, identity, options); // { ...options, onUpdate: this.store._updateIndex.bind(this.store) }
    }

    drop?(): Promise<void> {
        return this.store.drop();
    }
    load?(): Promise<void> {
        return this.store.load();
    }
    get name(): string {
        return this.store.name;
    }

    /*  clone(newName: string): StoreAccessController<A, S, T> {
         return new StoreAccessController<A, S, T>({
             store: this.store.clone(newName) as S
         })
     } */

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
        return load(ipfs, address, StoreAccessController, options)
    }

    get id(): string {
        return this.store.id;
    }
    get oplog(): Log<any> {
        return this.store.oplog;
    }
    get cache(): Cache {
        return this.store.cache;
    }

    get events(): EventEmitter {
        return this.store.events;
    }

}