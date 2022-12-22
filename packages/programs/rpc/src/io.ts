import { serialize, BorshError } from "@dao-xyz/borsh";
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
    PublicSignKey,
} from "@dao-xyz/peerbit-crypto";
import { Libp2p } from "libp2p";
import { Identity } from "@dao-xyz/peerbit-log";
import { RequestV0, ResponseV0, RPCMessage } from "./encoding.js";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
export const logger = loggerFn({ module: "rpc" });
export type RPCOptions = {
    signer?: Identity;
    encryption?: {
        key: GetEncryptionKeypair;
        responders?: (X25519PublicKey | Ed25519PublicKey)[];
    };
    amount?: number;
    timeout?: number;
    isTrusted?: (publicKey: MaybeSigned<any>) => Promise<boolean>;
    responseRecievers?: X25519PublicKey[];
    context?: string;
    strict?: boolean;
};

export const send = async (
    libp2p: Libp2p,
    topic: string,
    responseTopic: string,
    query: RequestV0,
    responseHandler: (response: ResponseV0, from?: PublicSignKey) => void,
    sendKey: X25519Keypair,
    options: RPCOptions = {}
) => {
    if (typeof options.timeout !== "number") {
        options.timeout = 30 * 1000;
    }

    // send query and wait for replies in a generator like behaviour
    let results = 0;
    const _responseHandler = async (evt: CustomEvent<Message>) => {
        //  if (evt.detail.type === "signed")
        {
            const message = evt.detail;
            if (message) {
                /*    if (message.from.equals(libp2p.peerId)) {
                       return;
                   } */
                try {
                    const { result, from } = await decryptVerifyInto(
                        message.data,
                        RPCMessage,
                        sendKey,
                        {
                            isTrusted: options?.isTrusted,
                        }
                    );

                    if (result instanceof ResponseV0) {
                        responseHandler(result, from);
                        results += 1;
                    }
                } catch (error) {
                    if (error instanceof AccessError) {
                        return; // Ignore things we can not open
                    }

                    if (error instanceof BorshError && !options.strict) {
                        logger.debug("Namespace error");
                        return; // Name space conflict most likely
                    }

                    console.error(
                        "failed ot deserialize query response",
                        error
                    );
                    throw error;
                }
            }
        }
    };
    try {
        libp2p.pubsub.subscribe(responseTopic);
        libp2p.pubsub.addEventListener("message", _responseHandler);
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

    await libp2p.pubsub.publish(topic, serialize(maybeEncryptedMessage));

    if (options.amount != undefined) {
        await waitFor(() => results >= (options.amount as number), {
            timeout: options.timeout,
            delayInterval: 500,
        });
    } else {
        await delay(options.timeout);
    }
    try {
        //  await libp2p.pubsub.unsubscribe(topic); TODO should we?
        await libp2p.pubsub.removeEventListener("message", _responseHandler);
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
    libp2p: Libp2p,
    responseTopic: string,
    request: RequestV0,
    response: ResponseV0,
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

    maybeEncryptedMessage = await decryptedMessage.encrypt(
        options.encryption.getEncryptionKeypair,
        request.respondTo
    );

    await libp2p.pubsub.publish(
        responseTopic,
        serialize(maybeEncryptedMessage)
    );
};
