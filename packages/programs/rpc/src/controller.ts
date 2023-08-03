import {
	AbstractType,
	BorshError,
	deserialize,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import {
	PublicSignKey,
	toBase64,
	AccessError,
	X25519PublicKey,
	X25519Keypair,
} from "@peerbit/crypto";
import { RequestV0, ResponseV0, RPCMessage } from "./encoding.js";
import { RPCOptions, logger, RPCResponse } from "./io.js";
import { Address } from "@peerbit/program";
import {
	DataEvent,
	PubSubData,
	PublishOptions,
} from "@peerbit/pubsub-interface";
import { Program } from "@peerbit/program";
import { DataMessage } from "@peerbit/stream-interface";
import pDefer, { DeferredPromise } from "p-defer";
import { waitFor } from "@peerbit/time";
import { equals } from "uint8arrays";
import { options } from "benchmark";

export type SearchContext = (() => Address) | Program;
export type CanRequest = (key?: PublicSignKey) => Promise<boolean> | boolean;

export type RPCSetupOptions<Q, R> = {
	topic: string;
	queryType: AbstractType<Q>;
	responseType: AbstractType<R>;
	canRequest?: CanRequest;
	responseHandler?: ResponseHandler<Q, R>;
	subscriptionData?: Uint8Array;
};
export type QueryContext = {
	from?: PublicSignKey;
	address: string;
};
export type ResponseHandler<Q, R> = (
	query: Q,
	context: QueryContext
) => Promise<R | undefined> | R | undefined;

const createValueResolver = <T>(
	type: AbstractType<T> | Uint8Array
): ((data: Uint8Array) => T) => {
	if ((type as any) === Uint8Array) {
		return (data) => data as T
	} else {
		return (data) => deserialize(data, type as AbstractType<T>);
	}
};

@variant("rpc")
export class RPC<Q, R> extends Program<RPCSetupOptions<Q, R>> {
	canRequest: CanRequest;

	private _subscribed = false;
	private _responseHandler?: ResponseHandler<Q, (R | undefined) | R>;
	private _responseResolver: Map<
		string,
		(properties: { response: ResponseV0; message: DataMessage }) => any
	>;
	private _requestType: AbstractType<Q> | Uint8ArrayConstructor;
	private _requestTypeIsUint8Array: boolean;
	private _responseType: AbstractType<R>;
	private _rpcTopic: string | undefined;
	private _onMessageBinded: ((arg: any) => any) | undefined = undefined;
	private _subscriptionMetaData: Uint8Array | undefined;

	private _keypair: X25519Keypair;

	private _getResponseValueFn: (data: Uint8Array) => R;
	private _getRequestValueFn: (data: Uint8Array) => Q;
	async open(args: RPCSetupOptions<Q, R>): Promise<void> {
		this._rpcTopic = args.topic ?? this._rpcTopic;
		this._responseHandler = args.responseHandler;
		this._requestType = args.queryType;
		this._requestTypeIsUint8Array = (this._requestType as any) === Uint8Array;
		this._responseType = args.responseType;
		this._responseResolver = new Map();
		this.canRequest = args.canRequest || (() => Promise.resolve(true));

		this._getResponseValueFn = createValueResolver(this._responseType);
		this._getRequestValueFn = createValueResolver(this._requestType);

		this._keypair = await X25519Keypair.create();
		await this.subscribe(args.subscriptionData);
	}

	private async _close(from?: Program): Promise<void> {
		if (this._subscribed) {
			await this.node.services.pubsub.unsubscribe(this.rpcTopic);
			await this.node.services.pubsub.removeEventListener(
				"data",
				this._onMessageBinded
			);
			this._subscribed = false;
		}
	}
	public async close(from?: Program): Promise<boolean> {
		const superClosed = await super.close(from);
		if (!superClosed) {
			return false;
		}
		await this._close(from);
		return true;
	}

	public async drop(from?: Program): Promise<boolean> {
		const superDropped = await super.drop(from);
		if (!superDropped) {
			return false;
		}
		await this._close(from);
		return true;
	}

	private _subscribing: Promise<void>;
	async subscribe(data = this._subscriptionMetaData): Promise<void> {
		await this._subscribing;
		if (
			this._subscribed &&
			(this._subscriptionMetaData === data ||
				(this._subscriptionMetaData &&
					data &&
					equals(this._subscriptionMetaData, data)))
		) {
			return;
		}

		const prevSubscriptionData = this._subscriptionMetaData;
		this._subscriptionMetaData = data;
		const wasSubscribed = this._subscribed;
		this._subscribed = true;

		this._onMessageBinded = this._onMessageBinded || this._onMessage.bind(this);

		if (wasSubscribed) {
			await this.node.services.pubsub.unsubscribe(this.rpcTopic, {
				data: prevSubscriptionData,
			});
		}

		this._subscribing = this.node.services.pubsub
			.subscribe(this.rpcTopic, { data })
			.then(() => {
				if (!wasSubscribed) {
					this.node.services.pubsub.addEventListener(
						"data",
						this._onMessageBinded!
					);
				}
			});

		await this._subscribing;
		if (!wasSubscribed) {
			await this.node.services.pubsub.requestSubscribers(this.rpcTopic);
		}
		logger.debug("subscribing to query topic (responses): " + this.rpcTopic);
	}

	async _onMessage(evt: CustomEvent<DataEvent>): Promise<void> {
		const { data, message } = evt.detail;

		if (data?.topics.find((x) => x === this.rpcTopic) != null) {
			try {
				const rpcMessage = deserialize(data.data, RPCMessage);
				if (rpcMessage instanceof RequestV0) {
					if (this._responseHandler) {
						const maybeEncrypted = rpcMessage.request;
						const decrypted = maybeEncrypted; // await maybeEncrypted.decrypt(this.node.keychain);

						if (!(await this.canRequest(message.sender))) {
							throw new AccessError();
						}

						const response = await this._responseHandler(
							this._getRequestValueFn(decrypted),
							{
								address: this.rpcTopic,
								from: message.sender,
							}
						);

						if (response && rpcMessage.respondTo) {
							// send query and wait for replies in a generator like behaviour

							await this.node.services.pubsub.publish(
								serialize(
									new ResponseV0({
										response: serialize(response),
										requestId: message.id,
									})
								),
								{
									topics: [this.rpcTopic],
									to: [message.sender],
									recievers: [rpcMessage.respondTo],
									encrypt: true,
									strict: true,
								}
							);
						}
					}
				} else if (rpcMessage instanceof ResponseV0) {
					const id = toBase64(rpcMessage.requestId);
					let handler = this._responseResolver.get(id);
					if (!handler) {
						handler = await waitFor(() => this._responseResolver.get(id));
					}
					handler!({
						message,
						response: rpcMessage,
					});
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

	private async createRequest(
		request: Q,
		respondTo?: X25519PublicKey
	) {

		/*const decryptedMessage = new DecryptedThing<Uint8Array>({
			data: requestData,
		});

		 let maybeEncryptedMessage: MaybeEncrypted<Uint8Array> = decryptedMessage;

		if (
			options?.encryption?.responders &&
			options?.encryption?.responders.length > 0
		) {
			maybeEncryptedMessage = await decryptedMessage.encrypt(
				options.encryption.key,
				...options.encryption.responders
			);
		} */


		return new RequestV0({
			request: this._requestTypeIsUint8Array
				? (request as Uint8Array)
				: serialize(request),
			respondTo,
		});;
	}

	private getPublishOptions(options?: PublishOptions): PublishOptions {
		return (options?.to
			? { ...options, to: options.to, strict: true, topics: [this.rpcTopic] }
			: { ...options, topics: [this.rpcTopic] }) as PublishOptions;
	}

	/**
	 * Send message and don't expect any response
	 * @param message
	 * @param options
	 */
	public async send(message: Q, options?: PublishOptions): Promise<void> {
		await this.node.services.pubsub.publish(
			serialize(await this.createRequest(message, undefined)),
			this.getPublishOptions(options)
		);
	}

	private createResponseHandler(
		promise: DeferredPromise<any>,
		keypair: X25519Keypair,
		allResults: RPCResponse<R>[],
		responders: Set<string>,
		expectedResponders?: Set<string>,
		options?: RPCOptions<R>
	) {
		return async (properties: {
			response: ResponseV0;
			message: DataMessage;
		}) => {
			try {
				const { response, message } = properties;
				const from = message.sender;

				if (options?.isTrusted && !(await options?.isTrusted(from))) {
					return;
				}

				const decrypted = response.response;
				const resultData = this._getResponseValueFn(decrypted);

				if (expectedResponders) {
					if (from && expectedResponders?.has(from.hashcode())) {
						options?.onResponse && options?.onResponse(resultData, from);
						allResults.push({ response: resultData, from });
						responders.add(from.hashcode());
						if (responders.size === expectedResponders.size) {
							promise.resolve();
						}
					}
				} else {
					options?.onResponse && options?.onResponse(resultData, from);
					allResults.push({ response: resultData, from });
					if (
						options?.amount != null &&
						allResults.length >= (options?.amount as number)
					) {
						promise.resolve();
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
				promise.reject(error);
			}
		};
	}

	/**
	 * Send a request and expect a response
	 * @param request
	 * @param options
	 * @returns
	 */
	public async request(
		request: Q,
		options?: RPCOptions<R>
	): Promise<RPCResponse<R>[]> {
		// We are generatinga new encryption keypair for each send, so we now that when we get the responses, they are encrypted specifcally for me, and for this request
		// this allows us to easily disregard a bunch of message just beacuse they are for a different reciever!
		const keypair = await X25519Keypair.create();

		// send query and wait for replies in a generator like behaviour
		let timeoutFn: any = undefined;

		const requestMessage = await this.createRequest(request, keypair.publicKey);
		const requestBytes = serialize(requestMessage);

		const allResults: RPCResponse<R>[] = [];

		const deferredPromise = pDefer();
		options?.stopper && options.stopper(deferredPromise.resolve);
		timeoutFn = setTimeout(() => {
			deferredPromise.resolve();
		}, options?.timeout || 10 * 1000);

		const expectedResponders =
			options?.to && options.to.length > 0
				? new Set(
					options.to.map((x) => (typeof x === "string" ? x : x.hashcode()))
				)
				: undefined;

		const responders = new Set<string>();

		const id = toBase64(
			await this.node.services.pubsub.publish(
				requestBytes,
				this.getPublishOptions(options)
			)
		);

		this._responseResolver.set(
			id,
			this.createResponseHandler(
				deferredPromise,
				keypair,
				allResults,
				responders,
				expectedResponders,
				options
			)
		);

		try {
			await deferredPromise.promise;
		} catch (error: any) {
			// timeout
			if (error.constructor.name != "TimeoutError") {
				throw new Error(
					"Got unexpected error when query: " + error.constructor.name
				);
			}
		} finally {
			clearTimeout(timeoutFn);
		}

		this._responseResolver.delete(id);
		return allResults;
	}

	public get rpcTopic(): string {
		if (!this._rpcTopic) {
			throw new Error("Not initialized");
		}
		return this._rpcTopic;
	}

	getTopics(): string[] {
		return [this.rpcTopic];
	}
}
