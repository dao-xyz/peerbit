import { field, variant } from '@dao-xyz/borsh';
import { BinaryDocumentStore, Operation } from '@dao-xyz/orbit-db-bdocstore';
import { getFromByToGenerator, RegionAccessController, RelationAccessController } from '@dao-xyz/orbit-db-trust-web';
import { Access, AccessData, AccessType } from './access';
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { PublicKey } from '@dao-xyz/identity';
import { Address, IInitializationOptions, StoreLike } from '@dao-xyz/orbit-db-store';
import { Log } from '@dao-xyz/ipfs-log';
import Cache from '@dao-xyz/orbit-db-cache';

@variant(0)
export class AccessStore implements StoreLike<Operation<any>> {

    @field({ type: BinaryDocumentStore })
    access: BinaryDocumentStore<AccessData>;


    @field({ type: RelationAccessController })
    identityGraphController: RelationAccessController;

    constructor(opts?: {
        name: string;
        rootTrust: PublicKey
    }) {
        if (opts) {
            this.access = new BinaryDocumentStore({
                indexBy: 'id',
                objectType: AccessData.name,
                accessController: new RegionAccessController({
                    name: opts.name + "_region",
                    rootTrust: opts.rootTrust
                })
            })

            this.identityGraphController = new RelationAccessController({ name: opts.name + '_identity' });
        }
    }

    get trust(): RegionAccessController {
        return this.access.accessController as RegionAccessController;
    }

    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async canRead(fromKey: PublicKey): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        const canReadCheck = async (key: PublicKey) => {
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
        for await (const trustedByKey of getFromByToGenerator(fromKey, this.identityGraphController.relationGraph)) {
            if (await canReadCheck(trustedByKey.from)) {
                return true;
            }
        }
        return false;
    }

    async canWrite(fromKey: PublicKey): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        const canWriteCheck = async (key: PublicKey) => {
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
        for await (const trustedByKey of getFromByToGenerator(fromKey, this.identityGraphController.relationGraph)) {
            if (await canWriteCheck(trustedByKey.from)) {
                return true;
            }
        }
        return false;
    }


    async init(ipfs, publicKey: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<Access>) {
        this.access._clazz = AccessData;
        /* await this.access.accessController.init(ipfs, publicKey, sign, options); */
        await this.identityGraphController.init(ipfs, publicKey, sign, options);
        return this.access.init(ipfs, publicKey, sign, options)
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
    sync(heads: Entry<Access>[]): Promise<void> {
        return this.access.sync(heads);
    }
    get replicationTopic(): string {
        return this.access.replicationTopic;
    }
    get events(): import("events") {
        return this.access.events;
    }
    get address(): Address {
        return this.access.address;
    }
    get oplog(): Log<Access> {
        return this.access.oplog;
    }
    get cache(): Cache {
        return this.access.cache;
    }
    get id(): string {
        return this.access.id;
    }
    get replicate(): boolean {
        return this.access.replicate;
    }
    getHeads(): Promise<Entry<Operation<any>>[]> {
        return this.access.getHeads();
    }
    get name(): string {
        return this.access.name;
    }

}