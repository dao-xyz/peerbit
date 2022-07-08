import { variant } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log';
import { BinaryDocumentStore, IBinaryDocumentStoreOptions } from '@dao-xyz/orbit-db-bdocstore';
import { SingleDBInterface } from '@dao-xyz/orbit-db-store-interface';
import { Access, AccessType } from './access';

@variant([0, 2])
export class ACLInterface extends SingleDBInterface<Access, BinaryDocumentStore<Access>>{

    /*  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: { verifiers: AccessVerifier[], canModifyAcaccess: (key: IdentitySerializable) => Promise<boolean>, grantAccess: (access: Access) => Promise<void>, revokeAccess: (access: Access) => Promise<void> } & IBinaryDocumentStoreOptions<Access>) {
         super(ipfs, id, dbname, options);
 
 
     } */

    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async allowed(entry: Entry<any>): Promise<boolean> {
        // TODO, improve, caching etc
        for (const value of Object.values(this.db._index._index)) {
            const access = value.payload.value;
            if (access.accessTypes.find((x) => x === AccessType.Admin)) {
                // check condition
                if (access.accessCondition.allowed(entry)) {
                    return true;
                }
                continue;
            }
        }
        return false;
    }

    /*  async subscribeForRequests(request:)
 
     async processAccessRequest(request: SignedAccessRequest, identities: Identities) {
         // verify 
         if (!await request.verifySignature(identities)) {
             return;
         }
     }
  */
    /*  static async modifyAccess(request: SignedAccessRequest, identity: Identity, ipfs: IPFSInstance) {
         await request.sign(identity);
         await ipfs.pubsub.publish(request.request.accessTopic, serialize(request));
     }
  */
    /*  public async close(): Promise<void> {
         await this._initializationPromise;
         await this._ipfs.pubsub.unsubscribe(this.queryTopic);
         this._subscribed = false;
         await super.close();
     } */
}