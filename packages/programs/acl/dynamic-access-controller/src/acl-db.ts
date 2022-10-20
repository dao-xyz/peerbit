import { field, variant } from '@dao-xyz/borsh';
import { DDocs, Operation } from '@dao-xyz/peerbit-ddoc';
import { getPathGenerator, TrustedNetwork, getFromByTo, RelationContract } from '@dao-xyz/peerbit-trusted-network';
import { Access, AccessData, AccessType } from './access';
import { Identity, Payload } from '@dao-xyz/ipfs-log'
import { MaybeEncrypted, PublicSignKey, SignatureWithKey } from '@dao-xyz/peerbit-crypto';

// @ts-ignore
import { v4 as uuid } from 'uuid';
import { IPFS } from 'ipfs-core-types';
import { DSearch } from '@dao-xyz/peerbit-dsearch';
import { Program, ProgramInitializationOptions } from '@dao-xyz/peerbit-program';
import { DQuery } from '@dao-xyz/peerbit-dquery';
import { IInitializationOptions } from '@dao-xyz/peerbit-dstore';

@variant([0, 12])
export class AccessStore extends Program {

    @field({ type: DDocs })
    access: DDocs<AccessData>;

    @field({ type: RelationContract })
    identityGraphController: RelationContract;

    @field({ type: TrustedNetwork })
    trustedNetwork: TrustedNetwork

    constructor(opts?: {
        name?: string;
        rootTrust?: PublicSignKey,
        trustedNetwork?: TrustedNetwork
    }) {
        super(opts);
        if (opts) {
            if (!opts.trustedNetwork && !opts.rootTrust) {
                throw new Error("Expecting either TrustedNetwork or rootTrust")
            }
            this.access = new DDocs({
                indexBy: 'id',
                objectType: AccessData.name,
                search: new DSearch({
                    query: new DQuery({})
                })
            })

            this.trustedNetwork = opts.trustedNetwork ? opts.trustedNetwork : new TrustedNetwork({
                name: (opts.name || uuid()) + "_region",
                rootTrust: opts.rootTrust as PublicSignKey
            })
            this.identityGraphController = new RelationContract({ name: 'relation', });
        }
    }



    // allow anyone write to the ACL db, but assume entry is invalid until a verifier verifies
    // can append will be anyone who has peformed some proof of work

    // or 

    // custom can append

    async canRead(s: SignatureWithKey | undefined): Promise<boolean> {
        // TODO, improve, caching etc

        if (!s) {
            return false;
        }


        // Check whether it is trusted by trust web
        if (await this.trustedNetwork.isTrusted(s.publicKey)) {
            return true;
        }

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

        if (await canReadCheck(s.publicKey)) {
            return true;
        }
        for await (const trustedByKey of getPathGenerator(s.publicKey, this.identityGraphController.relationGraph, getFromByTo)) {
            if (await canReadCheck(trustedByKey.from)) {
                return true;
            }
        }



        return false;
    }

    async canAppend(payload: MaybeEncrypted<Payload<any>>, key: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
        // TODO, improve, caching etc


        // Check whether it is trusted by trust web
        const signature = key.decrypted.getValue(SignatureWithKey)

        if (await this.trustedNetwork.isTrusted(signature.publicKey)) {
            return true;
        }
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
        if (await canWriteCheck(signature.publicKey)) {
            return true;
        }
        for await (const trustedByKey of getPathGenerator(signature.publicKey, this.identityGraphController.relationGraph, getFromByTo)) {
            if (await canWriteCheck(trustedByKey.from)) {
                return true;
            }
        }

        return false;
    }


    async init(ipfs: IPFS, identity: Identity, options: ProgramInitializationOptions): Promise<this> {
        this.access._clazz = AccessData;



        /* await this.access.accessController.init(ipfs, publicKey, sign, options); */
        await this.identityGraphController.init(ipfs, identity, { ...options, store: { ...options.store, canAppend: this.canAppend.bind(this) }, canRead: this.canRead.bind(this) });
        await this.access.init(ipfs, identity, { ...options, store: { ...options.store, canAppend: this.canAppend.bind(this) }, canRead: this.canRead.bind(this) })
        await this.trustedNetwork.init(ipfs, identity, { ...options })
        await super.init(ipfs, identity, options);
        return this;
    }
}