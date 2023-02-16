import {
	serialize,
	BorshError,
	AbstractType,
	deserialize,
} from "@dao-xyz/borsh";
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
	SignatureWithKey,
} from "@dao-xyz/peerbit-crypto";
import { Identity } from "@dao-xyz/peerbit-log";
import { RequestV0, ResponseV0, RPCMessage } from "./encoding.js";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { PubSubData } from "@dao-xyz/libp2p-direct-sub";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
export const logger = loggerFn({ module: "rpc" });
export type RPCOptions<R> = {
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
	to?: PublicSignKey[] | string[];
	onResponse?: (response: R, from?: PublicSignKey) => void;
	stopper?: (stopper: () => void) => void;
};

export type RPCResponse<R> = { response: R; from?: PublicSignKey };
export const send = async <R>(
	libp2p: Libp2pExtended,
	topic: string,
	responseTopic: string,
	query: RequestV0,
	responseType: AbstractType<R>,
	sendKey: X25519Keypair,
	options: RPCOptions<R> = {}
): Promise<RPCResponse<R>[]> => {
	if (typeof options.timeout !== "number") {
		options.timeout = 10 * 1000;
	}

	// send query and wait for replies in a generator like behaviour
	let timeoutFn: any = undefined;

	const serializedQuery = serialize(query);
	let maybeSignedMessage = new MaybeSigned<any>({ data: serializedQuery });
	if (options.signer) {
		maybeSignedMessage = await maybeSignedMessage.sign(
			options.signer.sign.bind(options.signer)
		);
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

	const allResults: RPCResponse<R>[] = [];
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
		const expectedResponders =
			options.to && options.to.length > 0
				? new Set(
						options.to.map((x) => (typeof x === "string" ? x : x.hashcode()))
				  )
				: undefined;
		const responders = new Set<string>();

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
						const resultData = deserialize(result.response, responseType);
						options.onResponse && options.onResponse(resultData, from);
						allResults.push({ response: resultData, from });

						if (
							options.amount != null &&
							allResults.length >= (options.amount as number)
						) {
							resolve!();
						}

						if (from && expectedResponders?.has(from.hashcode())) {
							responders.add(from.hashcode());
							if (responders.size === expectedResponders.size) {
								resolve();
							}
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
			libp2p.directsub.addEventListener("data", _responseHandler);
			libp2p.directsub.subscribe(responseTopic);
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

	const publicOptions = options.to ? { to: options.to } : { topics: [topic] };
	await libp2p.directsub.publish(
		serialize(maybeEncryptedMessage),
		publicOptions
	);
	await responsePromise;
	return allResults;
};

export const respond = async (
	libp2p: Libp2pExtended,
	responseTopic: string,
	request: RequestV0,
	response: ResponseV0,
	options: {
		signer?: (data: Uint8Array) => Promise<SignatureWithKey>;
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
		maybeSignedMessage = await maybeSignedMessage.sign(options.signer);
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
