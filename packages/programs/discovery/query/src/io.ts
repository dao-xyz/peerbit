import { serialize } from "@dao-xyz/borsh";
import type { Message } from "@libp2p/interface-pubsub";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import {
    MaybeSigned,
    decryptVerifyInto,
    DecryptedThing,
    MaybeEncrypted,
    AccessError,
    X25519PublicKey,
    Ed25519PublicKey,
    X25519Keypair,
    GetEncryptionKeypair,
    GetAnyKeypair,
    SignKey,
} from "@dao-xyz/peerbit-crypto";
import { IPFS } from "ipfs-core-types";
import { Identity } from "@dao-xyz/ipfs-log";
import { QueryRequestV0, QueryResponseV0, U64Compare } from "./query";

export type QueryOptions = {
    signer?: Identity;
    keyResolver?: GetAnyKeypair;
    encryption?: {
        key: GetEncryptionKeypair;
        responders?: (X25519PublicKey | Ed25519PublicKey)[];
    };
    waitForAmount?: number;
    maxAggregationTime?: number;
    isTrusted?: (publicKey: MaybeSigned<any>) => Promise<boolean>;
    responseRecievers?: X25519PublicKey[];
    context?: string;
    createdAt?: U64Compare[];
    modifiedAt?: U64Compare[];
};

export const query = async (
    ipfs: IPFS,
    topic: string,
    query: QueryRequestV0,
    responseHandler: (response: QueryResponseV0, from?: SignKey) => void,
    options: QueryOptions = {}
) => {
    if (typeof options.maxAggregationTime !== "number") {
        options.maxAggregationTime = 30 * 1000;
    }

    // send query and wait for replies in a generator like behaviour
    const responseTopic = query.getResponseTopic(topic);
    let results = 0;
    const _responseHandler = async (msg: Message) => {
        try {
            const { result, from } = await decryptVerifyInto(
                msg.data,
                QueryResponseV0,
                options.keyResolver || (() => Promise.resolve(undefined)),
                {
                    isTrusted: options?.isTrusted,
                }
            );
            responseHandler(result, from);
            results += 1;
        } catch (error) {
            if (error instanceof AccessError) {
                return; // Ignore things we can not open
            }

            console.error("failed ot deserialize query response", error);
            throw error;
        }
    };
    try {
        await ipfs.pubsub.subscribe(responseTopic, _responseHandler, {
            timeout: options.maxAggregationTime,
        });
    } catch (error: any) {
        // timeout
        if (error.constructor.name != "TimeoutError") {
            throw new Error(
                "Got unexpected error when query: " + error.constructor.name
            );
        }
    }
    const serializedQuery = serialize(query);
    let maybeSignedMessage = new MaybeSigned({ data: serializedQuery });

    if (options.signer) {
        maybeSignedMessage = await maybeSignedMessage.sign(async (data) => {
            return {
                publicKey: (options.signer as Identity).publicKey,
                signature: await (options.signer as Identity).sign(data),
            };
        });
    }

    const decryptedMessage = new DecryptedThing<MaybeSigned<Uint8Array>>({
        data: serialize(maybeSignedMessage),
    });
    let maybeEncryptedMessage: MaybeEncrypted<MaybeSigned<Uint8Array>> =
        decryptedMessage;
    if (
        options.encryption?.responders &&
        options.encryption?.responders.length > 0
    ) {
        maybeEncryptedMessage = await decryptedMessage.encrypt(
            options.encryption.key,
            ...options.encryption.responders
        );
    }

    await ipfs.pubsub.publish(topic, serialize(maybeEncryptedMessage));

    if (options.waitForAmount != undefined) {
        await waitFor(() => results >= (options.waitForAmount as number), {
            timeout: options.maxAggregationTime,
            delayInterval: 500,
        });
    } else {
        await delay(options.maxAggregationTime);
    }
    try {
        await ipfs.pubsub.unsubscribe(responseTopic, _responseHandler);
    } catch (error: any) {
        if (
            error?.constructor?.name === "NotStartedError" ||
            (typeof error?.message === "string" &&
                error?.message.indexOf("Pubsub is not started") !== -1)
        ) {
            return;
        }
        console.error("xxx", error);
        throw error;
    }
};

export const respond = async (
    ipfs: IPFS,
    topic: string,
    request: QueryRequestV0,
    response: QueryResponseV0,
    options: {
        signer?: Identity;
        encryption?: {
            getEncryptionKeypair: GetEncryptionKeypair;
        };
    } = {}
) => {
    if (!options.encryption) {
        options.encryption = {
            getEncryptionKeypair: () => X25519Keypair.create(),
        };
    }

    // send query and wait for replies in a generator like behaviour
    const serializedResponse = serialize(response);
    let maybeSignedMessage = new MaybeSigned({ data: serializedResponse });

    if (options.signer) {
        maybeSignedMessage = await maybeSignedMessage.sign(async (data) => {
            return {
                publicKey: (options.signer as Identity).publicKey,
                signature: await (options.signer as Identity).sign(data),
            };
        });
    }

    const decryptedMessage = new DecryptedThing<MaybeSigned<Uint8Array>>({
        data: serialize(maybeSignedMessage),
    });
    let maybeEncryptedMessage: MaybeEncrypted<MaybeSigned<Uint8Array>> =
        decryptedMessage;
    if (request.responseRecievers?.length > 0) {
        maybeEncryptedMessage = await decryptedMessage.encrypt(
            options.encryption.getEncryptionKeypair,
            ...request.responseRecievers
        );
    }
    await ipfs.pubsub.publish(
        request.getResponseTopic(topic),
        serialize(maybeEncryptedMessage)
    );
};
