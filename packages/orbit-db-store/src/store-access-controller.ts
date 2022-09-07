// TODO extend IPFS-LOG access controller interface for canAppend method
import { variant } from '@dao-xyz/borsh';
import { IInitializationOptions, IStoreOptions, Store } from './store';
import { AccessController } from './access-controller';
import { StoreLike } from './store-like';
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { Address, load } from './io';

@variant([0, 1])
export class StoreAccessController<S extends Store<any>> extends AccessController<S> implements StoreLike {

    store: S

    constructor(properties?: { store: S }) {
        super();
        if (properties) {
            this.store = properties.store;
        }
    }
    init(ipfs: any, identity: Identity, options: IInitializationOptions<any>): Promise<void> {
        return this.store.init(ipfs, identity, { ...options, onUpdate: this.store._updateIndex.bind(this.store) });
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
    set address(v: Address) {
        this.store.address = v;
    }

    get address(): Address {
        return this.store.address;
    }

    clone(newName: string): StoreAccessController<S> {
        return new StoreAccessController<S>({
            store: this.store.clone(newName) as S
        })
    }

    static load(ipfs: any, address: Address, options?: {
        timeout?: number;
    }) {
        return load(ipfs, address, StoreAccessController, options)
    }

}