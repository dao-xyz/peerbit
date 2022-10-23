import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { DDocs, Operation, PutOperation } from "@dao-xyz/peerbit-ddoc";
import { BORSH_ENCODING, Entry, Payload } from "@dao-xyz/ipfs-log";
import { createHash } from "crypto";
import { IPFSAddress, Key, OtherKey, PublicSignKey, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import type { PeerId } from '@libp2p/interface-peer-id';
import { MaybeEncrypted } from "@dao-xyz/peerbit-crypto";
import { DeleteOperation } from "@dao-xyz/peerbit-ddoc";
import { AnyRelation, createIdentityGraphStore, getPathGenerator, hasPath, Relation, getFromByTo, getToByFrom, hasRelation } from "./identity-graph";
import { BinaryPayload } from "@dao-xyz/peerbit-bpayload";
import { Program } from '@dao-xyz/peerbit-program';
import { DQuery } from "@dao-xyz/peerbit-dquery";
import { waitFor } from "@dao-xyz/peerbit-time";

const encoding = BORSH_ENCODING(Operation);

const canAppendByRelation = async (mpayload: MaybeEncrypted<Payload<Operation<any>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>, db: DDocs<Relation>, isTrusted?: (key: PublicSignKey) => Promise<boolean>): Promise<boolean> => {

    // verify the payload 
    const decrypted = (await mpayload.decrypt(db.store.oplog._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)))).decrypted;
    const payload = decrypted.getValue(Payload);
    const operation = payload.getValue(encoding);
    if (operation instanceof PutOperation || operation instanceof DeleteOperation) {
        /*  const relation: Relation = operation.value || deserialize(operation.data, Relation); */
        await keyEncrypted.decrypt(db.store.oplog._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)));
        const key = keyEncrypted.decrypted.getValue(SignatureWithKey).publicKey;

        if (operation instanceof PutOperation) {
            // TODO, this clause is only applicable when we modify the identityGraph, but it does not make sense that the canAppend method does not know what the payload will
            // be, upon deserialization. There should be known in the `canAppend` method whether we are appending to the identityGraph.

            const relation: BinaryPayload = operation._value || deserialize(operation.data, BinaryPayload);
            operation._value = relation;

            if (relation instanceof AnyRelation) {
                if (!relation.from.equals(key)) {
                    return false;
                }
            }

            // else assume the payload is accepted
        }

        if (isTrusted) {
            const trusted = await isTrusted(key);
            return trusted
        }
        else {
            return true;
        }
    }

    else {
        return false;
    }
}

@variant([0, 10])
export class RelationContract extends Program {

    @field({ type: DDocs })
    relationGraph: DDocs<Relation>

    constructor(props?: {
        name?: string,
        queryRegion?: string
    }) {
        super(props)
        if (props) {
            this.relationGraph = createIdentityGraphStore(props);
        }
    }

    async canAppend(payload: MaybeEncrypted<Payload<Operation<Relation>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {
        return canAppendByRelation(payload, keyEncrypted, this.relationGraph)
    }


    async setup(options?: { canRead?(key: SignatureWithKey): Promise<boolean> }) {
        await this.relationGraph.setup({ type: Relation, canAppend: this.canAppend.bind(this), canRead: options?.canRead }) // self referencing access controller
    }


    async addRelation(to: PublicSignKey/*  | Identity | IdentitySerializable */) {
        /*  trustee = PublicKey.from(trustee); */
        await this.relationGraph.put(new AnyRelation({
            to: to,
            from: this.relationGraph.store.identity.publicKey
        }));
    }
}

export class Message {

}
@variant(0)
export class RequestHeadsMessage extends Message { }

@variant(1)
export class HeadsMessages extends Message {

    @field({ type: vec(Entry) })
    heads: Entry<any>[]

    constructor(properties?: { heads: Entry<any>[] }) {
        super();
        if (properties) {
            this.heads = properties.heads;
        }
    }
}


/**
 * Not shardeable since we can not query trusted relations, because this would lead to a recursive problem where we then need to determine whether the responder is trusted or not
 */

@variant([0, 11])
export class TrustedNetwork extends Program {

    @field({ type: PublicSignKey })
    rootTrust: PublicSignKey

    @field({ type: DDocs })
    trustGraph: DDocs<Relation>

    @field({ type: DQuery })
    query: DQuery<RequestHeadsMessage, HeadsMessages>;

    constructor(props?: {
        name?: string,
        rootTrust: PublicSignKey,
        query?: DQuery<RequestHeadsMessage, HeadsMessages>
    }) {
        super(props);
        if (props) {
            this.trustGraph = createIdentityGraphStore(props);
            this.rootTrust = props.rootTrust;
            this.query = props.query || new DQuery({ queryAddressSuffix: 'heads' });
        }
    }


    async setup() {
        await this.trustGraph.setup({ type: Relation, canAppend: this.canAppend.bind(this), canRead: this.canRead.bind(this) }) // self referencing access controller
        await this.query.setup({ queryType: RequestHeadsMessage, responseType: HeadsMessages, responseHandler: this.exchangeHeads.bind(this), canRead: () => Promise.resolve(true) })
    }

    exchangeHeads(_query: RequestHeadsMessage): HeadsMessages | undefined {
        if (!this.trustGraph.store.replicate) {
            return undefined // we do this because we might not have all the heads
        }
        return new HeadsMessages({
            heads: this.trustGraph.store.oplog.heads
        });
    }


    async canAppend(payload: MaybeEncrypted<Payload<Operation<any>>>, keyEncrypted: MaybeEncrypted<SignatureWithKey>): Promise<boolean> {

        return canAppendByRelation(payload, keyEncrypted, this.trustGraph, async (key) => await this.isTrusted(key))
    }

    async canRead(key: SignatureWithKey | undefined): Promise<boolean> {
        if (!key) {
            return false;
        }
        return await this.isTrusted(key.publicKey);
    }

    async add(trustee: PublicSignKey | PeerId/*  | Identity | IdentitySerializable */) {
        /*  trustee = PublicKey.from(trustee); */
        if (!this.hasRelation(trustee, this.trustGraph.store.identity.publicKey)) {
            await this.trustGraph.put(new AnyRelation({
                to: trustee instanceof Key ? trustee : new IPFSAddress({ address: trustee.toString() }),
                from: this.trustGraph.store.identity.publicKey
            }));
        }
    }

    hasRelation(trustee: PublicSignKey | PeerId, truster = this.rootTrust) {
        return !!hasRelation(truster, trustee instanceof Key ? trustee : new IPFSAddress({ address: trustee.toString() }), this.trustGraph)[0]?.value;
    }




    /**
     * Follow trust path back to trust root.
     * Trust root is always trusted.
     * Hence if
     * Root trust A trust B trust C
     * C is trusted by Root
     * @param trustee 
     * @param truster, the truster "root", if undefined defaults to the root trust
     * @returns true, if trusted
     */
    async isTrusted(trustee: PublicSignKey | OtherKey, truster: PublicSignKey = this.rootTrust): Promise<boolean> {

        if (trustee.equals(this.rootTrust)) {
            return true;
        }
        if (this.trustGraph.store.replicate) {
            return this._isTrustedLocal(trustee, truster)
        }
        else {
            let trusted = false;
            this.query.query(new RequestHeadsMessage(), async (heads, from) => {
                if (!from) {
                    return;
                }

                const logs = await Promise.all(heads.heads.map(h => this.trustGraph.store._replicator._replicateLog(h)));
                await this.trustGraph.store.updateStateFromLogs(logs);

                const isTrustedSender = await this._isTrustedLocal(from, truster);
                if (!isTrustedSender) {
                    return;
                }


                const isTrustedTrustee = await this._isTrustedLocal(trustee, truster);
                if (isTrustedTrustee) {
                    trusted = true;
                }
            })

            try {
                await waitFor(() => trusted)
                return trusted;
            } catch (error) {
                return false;
            }

        }

    }

    async _isTrustedLocal(trustee: PublicSignKey | OtherKey, truster: PublicSignKey = this.rootTrust): Promise<boolean> {
        const trustPath = await hasPath(trustee, truster, this.trustGraph, getFromByTo);
        return !!trustPath
    }

    async getTrusted(): Promise<PublicSignKey[]> {
        let current = this.rootTrust;
        const participants: PublicSignKey[] = [current];
        let generator = getPathGenerator(current, this.trustGraph, getToByFrom);
        for await (const next of generator) {
            participants.push(next.to);
        }
        return participants;

    }

    hashCode(): string {
        return createHash('sha1').update(serialize(this)).digest('hex')
    }

}

