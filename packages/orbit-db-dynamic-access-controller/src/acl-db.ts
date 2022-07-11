import { field, option, variant } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { BinaryDocumentStore, IBinaryDocumentStoreOptions } from '@dao-xyz/orbit-db-bdocstore';
import { BStoreOptions } from '@dao-xyz/orbit-db-bstores';
import { IStoreOptions } from '@dao-xyz/orbit-db-store';
import { SingleDBInterface } from '@dao-xyz/orbit-db-store-interface';
import { P2PTrust, TRUST_WEB_ACCESS_CONTROLLER } from '@dao-xyz/orbit-db-trust-web';
import { Access, AccessType } from './access';

@variant([0, 2])
export class ACLInterface extends SingleDBInterface<Access, BinaryDocumentStore<Access>>{

    constructor(opts?: {
        name: string;
        address?: string;
        storeOptions: BStoreOptions<BinaryDocumentStore<Access>>
    }) {
        super(opts);
        if (opts) {
            Object.assign(this, opts);
        }
    }

    async init(orbitDB: OrbitDB, options: IStoreOptions<Access, any> & { trustResolver: () => P2PTrust }): Promise<void> {

        return await super.init(orbitDB, {
            ...options, accessController: {
                type: TRUST_WEB_ACCESS_CONTROLLER,
                trustResolver: options.trustResolver,
                skipManifest: true
            }
        })
    }


    async load(waitForReplicationEventsCount?: number) {
        await super.load(waitForReplicationEventsCount)
    }

    async close() {
        await super.close();
    }

    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async allowed(entry: Entry<any>): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        for (const value of Object.values(this.db._index._index)) {
            const access = value.payload.value;
            if (access.accessTypes.find((x) => x === AccessType.Admin) !== undefined) {
                // check condition
                if (access.accessCondition.allowed(entry)) {
                    return true;
                }
                continue;
            }
        }
        return false;
    }

}