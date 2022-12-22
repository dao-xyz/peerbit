import { variant, option, field, vec, serialize } from "@dao-xyz/borsh";
import { Entry, Identity } from "@dao-xyz/peerbit-log";
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { MaybeSigned } from "@dao-xyz/peerbit-crypto";
import { Program } from "@dao-xyz/peerbit-program";
import { fixedUint8Array } from "@dao-xyz/peerbit-borsh-utils";
import { Store } from "@dao-xyz/peerbit-store";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { TransportMessage } from "./message.js";
import { v4 as uuid } from "uuid";

const logger = loggerFn({ module: "exchange-heads" });

export class MinReplicas {
    get value(): number {
        throw new Error("Not implemented");
    }
}

@variant(0)
export class AbsolutMinReplicas extends MinReplicas {
    _value: number;
    constructor(value: number) {
        super();
        this._value = value;
    }
    get value() {
        return this._value;
    }
}

/**
 * This thing allows use to faster sync since we can provide
 * references that can be read concurrently to
 * the entry when doing Log.fromEntry or Log.fromEntryHash
 */
@variant(0)
export class EntryWithRefs<T> {
    @field({ type: Entry })
    entry: Entry<T>;

    @field({ type: vec(Entry) })
    references: Entry<T>[]; // are some parents to the entry

    constructor(properties: { entry: Entry<T>; references: Entry<T>[] }) {
        this.entry = properties.entry;
        this.references = properties.references;
    }
}

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends TransportMessage {
    @field({ type: "string" })
    topic: string;

    @field({ type: "string" })
    programAddress: string;

    @field({ type: "u32" })
    storeIndex: number;

    @field({ type: option("u32") })
    programIndex?: number;

    @field({ type: vec(EntryWithRefs) })
    heads: EntryWithRefs<T>[];

    @field({ type: option(MinReplicas) })
    minReplicas?: MinReplicas;

    @field({ type: fixedUint8Array(4) })
    reserved: Uint8Array = new Uint8Array(4);

    constructor(props: {
        topic: string;
        programIndex?: number;
        programAddress: string;
        storeIndex: number;
        heads: EntryWithRefs<T>[];
        minReplicas?: MinReplicas;
    }) {
        super();
        this.id = uuid();
        this.topic = props.topic;
        this.storeIndex = props.storeIndex;
        this.programIndex = props.programIndex;
        this.programAddress = props.programAddress;
        this.heads = props.heads;
        this.minReplicas = props.minReplicas;
    }
}

@variant([0, 1])
export class RequestHeadsMessage extends TransportMessage {
    @field({ type: "string" })
    topic: string;

    @field({ type: "string" })
    address: string;

    constructor(props: { topic: string; address: string }) {
        super();
        if (props) {
            this.topic = props.topic;
            this.address = props.address;
        }
    }
}

export const exchangeHeads = async (
    send: (id: string, message: Uint8Array) => Promise<any>,
    store: Store<any>,
    program: Program,
    heads: Entry<any>[],
    topic: string,
    includeReferences: boolean,
    identity?: Identity
) => {
    const headsSet = new Set(heads);
    const headsWithRefs = heads.map((head) => {
        const refs = !includeReferences
            ? []
            : store.oplog
                  .getPow2Refs(store.oplog.length)
                  .filter((r) => !headsSet.has(r)); // pick a proportional amount of refs so we can efficiently load the log. TODO should be equidistant for good performance?
        return new EntryWithRefs({
            entry: head,
            references: refs,
        });
    });
    logger.debug(`Send latest heads of '${store._storeIndex}'`);
    if (heads && heads.length > 0) {
        const message = new ExchangeHeadsMessage({
            topic: topic,
            storeIndex: store._storeIndex,
            programIndex: program._programIndex,
            programAddress: (program.address ||
                program.parentProgram.address)!.toString(),
            heads: headsWithRefs,
        });
        const maybeSigned = new MaybeSigned({ data: serialize(message) });
        let signedMessage: MaybeSigned<any> = maybeSigned;
        if (identity) {
            const signer = async (data: Uint8Array) => {
                return {
                    signature: await identity.sign(data),
                    publicKey: identity.publicKey,
                };
            };
            signedMessage = await signedMessage.sign(signer);
        }

        const decryptedMessage = new DecryptedThing({
            data: serialize(signedMessage),
        }); // TODO encryption?
        const serializedMessage = serialize(decryptedMessage);
        await send(message.id, serializedMessage);
    }
};
