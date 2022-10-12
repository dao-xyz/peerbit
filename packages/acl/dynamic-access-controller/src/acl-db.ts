import { field, variant } from '@dao-xyz/borsh';
import { BinaryDocumentStore, Operation } from '@dao-xyz/orbit-db-bdocstore';
import { getPathGenerator, TrustedNetwork, RelationAccessController, getFromByTo } from '@dao-xyz/peerbit-trusted-network';
import { Access, AccessData, AccessType } from './access';
import { Entry, Identity } from '@dao-xyz/ipfs-log'
import { PublicSignKey } from '@dao-xyz/peerbit-crypto';
import { Address, EntryWithRefs, IInitializationOptions, StoreLike } from '@dao-xyz/orbit-db-store';
import { Log } from '@dao-xyz/ipfs-log';
// @ts-ignore
import { v4 as uuid } from 'uuid';
import { IPFS } from 'ipfs-core-types';

@variant(0)
export class AccessStore implements StoreLike<Operation<any>> {

    @field({ type: BinaryDocumentStore })
    access: BinaryDocumentStore<AccessData>;

    @field({ type: RelationAccessController })
    identityGraphController: RelationAccessController;

    constructor(opts?: {
        name?: string;
        rootTrust?: PublicSignKey,
        trustedNetwork?: TrustedNetwork
    }) {
        if (opts) {
            if (!opts.trustedNetwork && !opts.rootTrust) {
                throw new Error("Expecting either TrustedNetwork or rootTrust")
            }
            this.access = new BinaryDocumentStore({
                indexBy: 'id',
                objectType: AccessData.name,
                accessController: opts.trustedNetwork ? opts.trustedNetwork : new TrustedNetwork({
                    name: (opts.name || uuid()) + "_region",
                    rootTrust: opts.rootTrust as PublicSignKey
                })
            })

            this.identityGraphController = new RelationAccessController({ name: opts.name + '_identity' });
        }
    }

    get trust(): TrustedNetwork {
        return this.access.accessController as TrustedNetwork;
    }

    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async canRead(fromKey: PublicSignKey): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        const canReadCheck = async (key: PublicSignKey) => {
            for (const value of Object.values(this.access._index._index)) {
                const access = value.value;
                if (access instanceof Access) {
                    if (access.accessTypes.find((x) => x === AccessType.Any || x === AccessType.Read) !== undefined) {
                        // check condition
                        if (await access.accessCondition.allowed(key)) {
                            return true;
                        }
                        continue;
                    }
                }
            }
        }

        if (await canReadCheck(fromKey)) {
            return true;
        }
        for await (const trustedByKey of getPathGenerator(fromKey, this.identityGraphController.relationGraph, getFromByTo)) {
            if (await canReadCheck(trustedByKey.from)) {
                return true;
            }
        }
        return false;
    }

    async canWrite(fromKey: PublicSignKey): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        const canWriteCheck = async (key: PublicSignKey) => {
            for (const value of Object.values(this.access._index._index)) {
                const access = value.value
                if (access instanceof Access) {
                    if (access.accessTypes.find((x) => x === AccessType.Any || x === AccessType.Write) !== undefined) {
                        // check condition
                        if (await access.accessCondition.allowed(key)) {
                            return true;
                        }
                        continue;
                    }
                }

            }
        }
        if (await canWriteCheck(fromKey)) {
            return true;
        }
        for await (const trustedByKey of getPathGenerator(fromKey, this.identityGraphController.relationGraph, getFromByTo)) {
            if (await canWriteCheck(trustedByKey.from)) {
                return true;
            }
        }
        return false;
    }


    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<Operation<Access>>): Promise<AccessStore> {
        this.access._clazz = AccessData;

        const store = await options.saveAndResolveStore(ipfs, this);
        if (store !== this) {
            return store as AccessStore;
        }

        /* await this.access.accessController.init(ipfs, publicKey, sign, options); */
        await this.identityGraphController.init(ipfs, identity, options);
        await this.access.init(ipfs, identity, options)
        return this;
    }

    close(): Promise<void> {
        return this.access.close();
    }
    drop(): Promise<void> {
        return this.access.drop();
    }
    load(): Promise<void> {
        return this.access.load();
    }
    save(ipfs: any, options?: { format?: string; pin?: boolean; timeout?: number; }) {
        return this.access.save(ipfs, options);
    }
    sync(heads: (Entry<Operation<Access>> | EntryWithRefs<Operation<Access>>)[]): Promise<void> {
        return this.access.sync(heads);
    }


    get address(): Address {
        return this.access.address;
    }
    get oplog(): Log<Operation<Access>> {
        return this.access.oplog;
    }
    get id(): string {
        return this.access.id;
    }
    get replicate(): boolean {
        return this.access.replicate;
    }

    get name(): string {
        return this.access.name;
    }

}