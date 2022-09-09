import { field, variant } from '@dao-xyz/borsh';
import { BinaryDocumentStore, Operation } from '@dao-xyz/orbit-db-bdocstore';
import { Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';
import { TrustWebAccessController } from '@dao-xyz/orbit-db-trust-web';
import { Access, AccessData, AccessType } from './access';
import { Entry, Payload } from '@dao-xyz/ipfs-log-entry'
import { MaybeEncrypted } from '@dao-xyz/encryption-utils';
import { PublicKey } from '@dao-xyz/identity';
import { Address, IInitializationOptions, Store, StoreLike } from '@dao-xyz/orbit-db-store';
import { Log } from '@dao-xyz/ipfs-log';
import Cache from '@dao-xyz/orbit-db-cache';
import { IPFS } from 'ipfs-core-types/src/index';
import { Ed25519PublicKey } from 'sodium-plus';


@variant(0)
export class ACLInterface implements StoreLike<Operation> {

    @field({ type: Store })
    store: BinaryDocumentStore<AccessData>;


    rootTrust: TrustWebAccessController;

    constructor(opts?: {
        name: string;
        rootTrust: PublicKey | Identity | IdentitySerializable
    }) {
        if (opts) {
            this.store = new BinaryDocumentStore({
                indexBy: 'id',
                objectType: AccessData.name,
                accessController: new TrustWebAccessController({
                    name: opts.name,
                    rootTrust: opts.rootTrust,
                    /* skipManifest: true,
                    appendAll: opts.appendAll, */
                })
            })

        }
    }

    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async canRead(entry: MaybeEncrypted<Payload<any>>, key: Ed25519PublicKey): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        for (const value of Object.values(this.store._index._index)) {
            const access = value.value;
            if (access instanceof Access) {
                if (access.accessTypes.find((x) => x === AccessType.Any || x === AccessType.Read) !== undefined) {
                    // check condition
                    if (access.accessCondition.allowed(entry, key)) {
                        return true;
                    }
                    continue;
                }
            }
        }
        return false;
    }

    async canWrite(entry: MaybeEncrypted<Payload<any>>, identity: MaybeEncrypted<IdentitySerializable>): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        for (const value of Object.values(this.store._index._index)) {
            const access = value.value
            if (access instanceof Access) {
                if (access.accessTypes.find((x) => x === AccessType.Any || x === AccessType.Write) !== undefined) {
                    // check condition
                    if (access.accessCondition.allowed(entry, identity)) {
                        return true;
                    }
                    continue;
                }
            }

        }
        return false;
    }


    async init(ipfs: IPFS<{}>, identity: Identity, options: IInitializationOptions<any>): Promise<void> {
        this.rootTrust = this.store.access as TrustWebAccessController;
        this.store._clazz = AccessData;
        await this.store.access.init(ipfs, identity, options);
        return this.store.init(ipfs, identity, options)
    }

    close(): Promise<void> {
        return this.store.close();
    }
    drop(): Promise<void> {
        return this.store.drop();
    }
    load(): Promise<void> {
        return this.store.load();
    }
    save(ipfs: any, options?: { format?: string; pin?: boolean; timeout?: number; }) {
        return this.store.save(ipfs, options);
    }
    sync(heads: Entry<Access>[]): Promise<void> {
        return this.store.sync(heads);
    }
    get replicationTopic(): string {
        return this.store.replicationTopic;
    }
    get events(): import("events") {
        return this.store.events;
    }
    get address(): Address {
        return this.store.address;
    }
    get oplog(): Log<Access> {
        return this.store.oplog;
    }
    get cache(): Cache {
        return this.store.cache;
    }
    get id(): string {
        return this.store.id;
    }
    get replicate(): boolean {
        return this.store.replicate;
    }
    getHeads(): Promise<Entry<Operation>[]> {
        return this.store.getHeads();
    }
    get name(): string {
        return this.store.name;
    }

}