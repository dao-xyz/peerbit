import { variant } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { BinaryDocumentStore, BinaryDocumentStoreOptions } from '@dao-xyz/orbit-db-bdocstore';
import { IQueryStoreOptions } from '@dao-xyz/orbit-db-query-store';
import { SingleDBInterface } from '@dao-xyz/orbit-db-store-interface';
import { P2PTrust, TRUST_WEB_ACCESS_CONTROLLER } from '@dao-xyz/orbit-db-trust-web';
import { Access, AccessData, AccessType } from './access';

export type ACLInterfaceOptions = IQueryStoreOptions<Access, any, any> & {
    trustResolver: () => P2PTrust, appendAll: boolean, subscribeToQueries: boolean,
    cache: boolean,
    create: boolean,
    replicate: boolean,
    directory: string
};

@variant([0, 2])
export class ACLInterface extends SingleDBInterface<Access, BinaryDocumentStore<Access>>{

    constructor(opts?: {
        name: string;
        address?: string;
    }) {
        super({
            ...opts, storeOptions: new BinaryDocumentStoreOptions<Access>({
                indexBy: 'id',
                objectType: AccessData.name
            })
        });
    }

    async init(orbitDB: OrbitDB, options: ACLInterfaceOptions): Promise<void> {
        options = {
            ...options,
            queryRegion: undefined,// Prevent query region to be set (will fallback to db specific queries (not global))
            accessController: {
                type: TRUST_WEB_ACCESS_CONTROLLER,
                trustResolver: options.trustResolver,
                skipManifest: true,
                appendAll: options.appendAll,
                storeOptions: {
                    subscribeToQueries: options.subscribeToQueries,
                    cache: options.subscribeToQueries,
                    create: options.create,
                    replicate: options.replicate,
                    directory: options.directory
                }
            }
        }
        return await super.init(orbitDB, options)
    }

    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async allowed(entry: Entry<any>): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        for (const value of Object.values(this.db._index._index)) {
            const access = value.value;
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