import {
	AbstractType,
	BorshError,
	deserialize,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import {
	DecryptedThing,
	MaybeEncrypted,
	MaybeSigned,
	PublicSignKey,
	toBase64,
} from "@dao-xyz/peerbit-crypto";
import { AccessError, decryptVerifyInto } from "@dao-xyz/peerbit-crypto";
import { RequestV0, ResponseV0, RPCMessage } from "./encoding.js";
import { RPCOptions, logger, RPCResponse } from "./io.js";
import {
	AbstractProgram,
	Address,
	ComposableProgram,
	ProgramInitializationOptions,
	ReplicatorType,
} from "@dao-xyz/peerbit-program";
import { Identity } from "@dao-xyz/peerbit-log";
import { X25519Keypair } from "@dao-xyz/peerbit-crypto";
import { PubSubData } from "@dao-xyz/libp2p-direct-sub";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";

export type SearchContext = (() => Address) | AbstractProgram;
export type CanRead = (key?: PublicSignKey) => Promise<boolean> | boolean;

export type RPCSetupOptions<Q, R> = {
	topic: string;
	queryType: AbstractType<Q>;
	responseType: AbstractType<R>;
	canRead?: CanRead;
	responseHandler: ResponseHandler<Q, R>;
};
export type QueryContext = {
	from?: PublicSignKey;
	address: string;
};
export type ResponseHandler<Q, R> = (
	query: Q,
	context: QueryContext
) => Promise<R | undefined> | R | undefined;

@variant("rpc")
export class RPC<Q, R> extends ComposableProgram {
	canRead: CanRead;

	private _subscribedResponses = false;
	private _subscribedRequests = false;
	private _onRequestBinded: (evt: CustomEvent<PubSubData>) => any;
	private _onResponseBinded: (evt: CustomEvent<PubSubData>) => any;
	private _responseHandler: ResponseHandler<Q, (R | undefined) | R>;
	private _responseResolver: Map<string, (request: ResponseV0) => any>;
	private _requestType: AbstractType<Q> | Uint8ArrayConstructor;
	private _responseType: AbstractType<R>;
	private _rpcTopic: string | undefined;

	async init(
		libp2p: Libp2pExtended,
		identity: Identity,
		options: ProgramInitializationOptions
	): Promise<this> {
		await super.init(libp2p, identity, options);
		if (this.role instanceof ReplicatorType) {
			await this._subscribeRequests();
		}
		return this;
	}

	public async setup(options: RPCSetupOptions<Q, R>) {
		this._rpcTopic = options.topic ?? this._rpcTopic;
		this._responseHandler = options.responseHandler;
		this._requestType = options.queryType;
		this._responseType = options.responseType;
		this._responseResolver = new Map();
		this.canRead = options.canRead || (() => Promise.resolve(true));
	}

	public async close(): Promise<boolean> {
		if (this._subscribedResponses) {
			await this.libp2p.services.pubsub.unsubscribe(this.rpcTopic);
			await this.libp2p.services.pubsub.removeEventListener(
				"data",
				this._onRequestBinded
			);
			this._subscribedResponses = false;
		}

		if (this._subscribedRequests) {
			this._responseResolver = undefined as any;
			await this.libp2p.services.pubsub.unsubscribe(this.rpcTopic);
			await this.libp2p.services.pubsub.removeEventListener(
				"data",
				this._onResponseBinded
			);
			this._subscribedRequests = false;
		}
		return super.close();
	}

	private async _subscribeRequests(): Promise<void> {
		if (this._subscribedRequests) {
			return;
		}

		this._onRequestBinded = this._onRequest.bind(this);
		this.libp2p.services.pubsub.addEventListener("data", this._onRequestBinded);
		await this.libp2p.services.pubsub.subscribe(this.rpcTopic);
		logger.debug("subscribing to query topic (requests): " + this.rpcTopic);
		this._subscribedRequests = true;
	}

	private _subscribing: Promise<void>;
	private async _subscribeResponses(): Promise<void> {
		await this._subscribing;
		if (this._subscribedResponses) {
			return;
		}
		this._subscribedResponses = true;
		this._subscribing = this.libp2p.services.pubsub
			.subscribe(this.rpcTopic)
			.then(() => {
				this._onResponseBinded = this._onResponse.bind(this);
				this.libp2p.services.pubsub.addEventListener(
					"data",
					this._onResponseBinded
				);
			});
		await this._subscribing;
		logger.debug("subscribing to query topic (responses): " + this.rpcTopic);
	}

	async _onRequest(evt: CustomEvent<PubSubData>): Promise<void> {
		const message = evt.detail;

		if (message?.topics.find((x) => x === this.rpcTopic) != null) {
			try {
				const request = deserialize(message.data, RPCMessage);
				if (request instanceof RequestV0) {
					const maybeEncrypted = deserialize<MaybeEncrypted<MaybeSigned<any>>>(
						request.request,
						MaybeEncrypted
					);
					const decrypted = await maybeEncrypted.decrypt(
						this.encryption?.getAnyKeypair
					);
					const maybeSigned = decrypted.getValue(MaybeSigned);
					if (!(await maybeSigned.verify())) {
						throw new AccessError();
					}

					if (!(await this.canRead(maybeSigned.signature?.publicKey))) {
						throw new AccessError();
					}

					const requestData =
						this._requestType === Uint8Array
							? (maybeSigned.data as Q)
							: deserialize(
									maybeSigned.data,
									this._requestType as AbstractType<Q>
							  );

					const response = await this._responseHandler(requestData, {
						address: this.rpcTopic,
						from: maybeSigned.signature!.publicKey,
					});

					if (response) {
						const encryption = this.encryption || {
							getEncryptionKeypair: () => X25519Keypair.create(),
						};

						// send query and wait for replies in a generator like behaviour
						const serializedResponse = serialize(response);
						let maybeSignedMessage = new MaybeSigned({
							data: serializedResponse,
						});

						// we use the peerId/libp2p identity for signatures, since we want to be able to send a message
						// with pubsub with a certain reciever. If we use (this.identity) we are going to use an identity
						// that is now known in the .pubsub network, hence the message might not be delivired if we
						// send with { to: [RECIEVER] } param
						maybeSignedMessage = await maybeSignedMessage.sign(
							this.libp2p.services.pubsub.sign.bind(this.libp2p.services.pubsub)
						);

						const decryptedMessage = new DecryptedThing<
							MaybeSigned<Uint8Array>
						>({
							data: serialize(maybeSignedMessage),
						});
						let maybeEncryptedMessage: MaybeEncrypted<MaybeSigned<Uint8Array>> =
							decryptedMessage;

						maybeEncryptedMessage = await decryptedMessage.encrypt(
							encryption.getEncryptionKeypair,
							request.respondTo
						);

						await this.libp2p.services.pubsub.publish(
							serialize(
								new ResponseV0({
									response: serialize(maybeEncryptedMessage),
									requestId: request.id,
								})
							),
							{
								topics: [this.rpcTopic],
								to: [maybeSigned.signature!.publicKey.hashcode()],
								strict: true,
							}
						);
					}
				} else {
					return;
				}
			} catch (error: any) {
				if (error instanceof AccessError) {
					logger.debug("Got message I could not decrypt");
					return;
				}

				if (error instanceof BorshError) {
					logger.error("Got message for a different namespace");
					return;
				}
				logger.error(
					"Error handling query: " +
						(error?.message ? error?.message?.toString() : error)
				);
			}
		}
	}
	async _onResponse(evt: CustomEvent<PubSubData>): Promise<void> {
		const message = evt.detail;

		if (message?.topics.find((x) => x === this.rpcTopic) != null) {
			try {
				const rpcMessage = deserialize(message.data, RPCMessage);

				if (rpcMessage instanceof ResponseV0) {
					this._responseResolver.get(toBase64(rpcMessage.requestId))?.(
						rpcMessage
					);
				}
			} catch (error) {
				if (error instanceof BorshError) {
					logger.debug("Namespace error");
					return; // Name space conflict most likely
				}
				logger.error("failed ot deserialize query response", error);
			}
		}
	}

	public async send(
		request: Q,
		options?: RPCOptions<R>
	): Promise<RPCResponse<R>[]> {
		// We are generatinga new encryption keypair for each send, so we now that when we get the responses, they are encrypted specifcally for me, and for this request
		// this allows us to easily disregard a bunch of message just beacuse they are for a different reciever!
		const keypair = await X25519Keypair.create();
		const requestData =
			(this._requestType as any) === Uint8Array
				? (request as Uint8Array)
				: serialize(request);

		const timeout = options?.timeout || 10 * 1000;

		// send query and wait for replies in a generator like behaviour
		let timeoutFn: any = undefined;
		let maybeSignedMessage = new MaybeSigned<any>({ data: requestData });
		maybeSignedMessage = await maybeSignedMessage.sign(
			this.libp2p.services.pubsub.sign.bind(this.libp2p.services.pubsub)
		);

		const decryptedMessage = new DecryptedThing<MaybeSigned<Uint8Array>>({
			data: serialize(maybeSignedMessage),
		});

		let maybeEncryptedMessage: MaybeEncrypted<MaybeSigned<Uint8Array>> =
			decryptedMessage;
		if (
			options?.encryption?.responders &&
			options?.encryption?.responders.length > 0
		) {
			maybeEncryptedMessage = await decryptedMessage.encrypt(
				options.encryption.key,
				...options.encryption.responders
			);
		}

		const requestMessage = new RequestV0({
			request: serialize(maybeEncryptedMessage),
			respondTo: keypair.publicKey,
		});
		const requestBytes = serialize(requestMessage);
		const requetsMessageIdString = toBase64(requestMessage.id);
		const allResults: RPCResponse<R>[] = [];

		await this._subscribeResponses();

		const publicOptions = options?.to
			? { to: options.to, strict: true, topics: [this.rpcTopic] }
			: { topics: [this.rpcTopic] };

		const responsePromise = new Promise<void>((rs, rj) => {
			const resolve = () => {
				timeoutFn && clearTimeout(timeoutFn);
				rs();
			};
			options?.stopper && options.stopper(resolve);
			const reject = (error) => {
				logger.error(error?.message);
				timeoutFn && clearTimeout(timeoutFn);
				rs();
			};
			const expectedResponders =
				options?.to && options.to.length > 0
					? new Set(
							options.to.map((x) => (typeof x === "string" ? x : x.hashcode()))
					  )
					: undefined;
			const responders = new Set<string>();
			const _responseHandler = async (response: ResponseV0) => {
				try {
					const { result: resultData, from } = await decryptVerifyInto(
						response.response,
						this._responseType,
						keypair,
						{
							isTrusted: options?.isTrusted,
						}
					);

					if (expectedResponders) {
						if (from && expectedResponders?.has(from.hashcode())) {
							options?.onResponse && options.onResponse(resultData, from);
							allResults.push({ response: resultData, from });
							responders.add(from.hashcode());
							if (responders.size === expectedResponders.size) {
								resolve();
							}
						}
					} else {
						options?.onResponse && options.onResponse(resultData, from);
						allResults.push({ response: resultData, from });
						if (
							options?.amount != null &&
							allResults.length >= (options.amount as number)
						) {
							resolve!();
						}
					}
				} catch (error) {
					if (error instanceof AccessError) {
						return; // Ignore things we can not open
					}

					if (error instanceof BorshError && !options?.strict) {
						logger.debug("Namespace error");
						return; // Name space conflict most likely
					}

					console.error("failed ot deserialize query response", error);
					reject(error);
				}
			};
			try {
				this._responseResolver.set(requetsMessageIdString, _responseHandler);
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
			}, timeout);
		});

		await this.libp2p.services.pubsub.publish(requestBytes, publicOptions);
		await responsePromise;
		this._responseResolver.delete(requetsMessageIdString);
		return allResults;
	}

	public get rpcTopic(): string {
		if (!this._rpcTopic) {
			throw new Error("Not initialized");
		}
		return this._rpcTopic;
	}
}
