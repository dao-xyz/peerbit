import { serialize, BorshError } from "@dao-xyz/borsh";
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
import { Identity } from "@dao-xyz/peerbit-log";
import { RequestV0, ResponseV0, RPCMessage } from "./encoding.js";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { PubSubData } from "@dao-xyz/libp2p-direct-sub";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
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
	stopper?: (stopper: () => void) => void;
};

export const send = async (
	libp2p: Libp2pExtended,
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
	let timeoutFn: any = undefined;

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

	const responsePromise = new Promise<void>((rs, rj) => {
		const resolve = () => {
			timeoutFn && clearTimeout(timeoutFn);
			if (libp2p.directsub.started) {
				libp2p.directsub.unsubscribe(responseTopic);
				libp2p.directsub.removeEventListener("data", _responseHandler);
			}
			rs();
		};
		options.stopper && options.stopper(resolve);

		const reject = (error) => {
			timeoutFn && clearTimeout(timeoutFn);
			if (libp2p.directsub.started) {
				libp2p.directsub.unsubscribe(responseTopic);
				libp2p.directsub.removeEventListener("data", _responseHandler);
			}
			rj(error);
		};
		const _responseHandler = async (evt: CustomEvent<PubSubData>) => {
			const message = evt.detail;
			if (message && message.topics.includes(responseTopic)) {
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

						if (
							options.amount != null &&
							results >= (options.amount as number)
						) {
							resolve!();
						}
					}
				} catch (error) {
					if (error instanceof AccessError) {
						return; // Ignore things we can not open
					}

					if (error instanceof BorshError && !options.strict) {
						logger.debug("Namespace error");
						return; // Name space conflict most likely
					}

					console.error("failed ot deserialize query response", error);
					reject(error);
				}
			}
		};
		try {
			libp2p.directsub.subscribe(responseTopic);
			libp2p.directsub.addEventListener("data", _responseHandler);
		} catch (error: any) {
			// timeout
			if (error.constructor.name != "TimeoutError") {
				throw new Error(
					"Got unexpected error when query: " + error.constructor.name
				);
			}
		}
		timeoutFn = setTimeout(() => {
			resolve();
		}, options.timeout);
	});

	await libp2p.directsub.publish(serialize(maybeEncryptedMessage), {
		topics: [topic],
	});

	await responsePromise;
};

export const respond = async (
	libp2p: Libp2pExtended,
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

	await libp2p.directsub.publish(serialize(maybeEncryptedMessage), {
		topics: [responseTopic],
	});
};
