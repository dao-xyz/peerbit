import { variant } from '@dao-xyz/borsh';
import { OrbitDB } from '@dao-xyz/orbit-db';
import { BinaryDocumentStore, IBStoreOptions } from '@dao-xyz/orbit-db-bdocstore';
import { Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider';
import { TrustWebAccessController } from '@dao-xyz/orbit-db-trust-web';
import { Access, AccessData, AccessType } from './access';
import { Payload } from '@dao-xyz/ipfs-log-entry'
import { MaybeEncrypted } from '@dao-xyz/encryption-utils';
import { PublicKey } from '@dao-xyz/identity';


@variant([0, 2])
export class ACLInterface extends BinaryDocumentStore<Access> {

    rootTrust: TrustWebAccessController;
    constructor(opts?: {
        name: string;
        appendAll: boolean,
        rootTrust: PublicKey | Identity | IdentitySerializable
    }) {
        super(opts ? {
            indexBy: 'id',
            objectType: AccessData.name,
            accessController: new TrustWebAccessController({
                name: opts.name,
                rootTrust: opts.rootTrust,
                /* skipManifest: true,
                appendAll: opts.appendAll, */
            })
        } : undefined);
        this.rootTrust = this.access as TrustWebAccessController;
    }

    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async canRead(entry: MaybeEncrypted<Payload<any>>, identity: MaybeEncrypted<IdentitySerializable>): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        for (const value of Object.values(this._index)) {
            const access = value.value;
            if (access.accessTypes.find((x) => x === AccessType.Any || x === AccessType.Read) !== undefined) {
                // check condition
                if (access.accessCondition.allowed(entry, identity)) {
                    return true;
                }
                continue;
            }
        }
        return false;
    }

    async canWrite(entry: MaybeEncrypted<Payload<any>>, identity: MaybeEncrypted<IdentitySerializable>): Promise<boolean> {
        // TODO, improve, caching etc

        // Else check whether its trusted by this access controller
        for (const value of Object.values(this._index)) {
            const access = value.value;
            if (access.accessTypes.find((x) => x === AccessType.Any || x === AccessType.Write) !== undefined) {
                // check condition
                if (access.accessCondition.allowed(entry, identity)) {
                    return true;
                }
                continue;
            }
        }
        return false;
    }

}