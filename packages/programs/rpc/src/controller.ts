import {
	type AbstractType,
	BorshError,
	deserialize,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import {
	AccessError,
	DecryptedThing,
	MaybeEncrypted,
	PublicSignKey,
	X25519Keypair,
	X25519PublicKey,
	randomBytes,
	toBase64,
} from "@peerbit/crypto";
import { Program, type ProgramEvents } from "@peerbit/program";
import {
	DataEvent,
	type PublishOptions as PubSubPublishOptions,
} from "@peerbit/pubsub-interface";
import {
	createRequestTransportContext,
	DataMessage,
	type PriorityOptions,
	type RequestTransportContext,
	SilentDelivery,
	type WithExtraSigners,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError } from "@peerbit/time";
import pDefer, { type DeferredPromise } from "p-defer";
import { RPCMessage, RequestV0, ResponseV0 } from "./encoding.js";
import {
	type EncryptionOptions,
	type RPCRequestOptions,
	type RPCResponse,
	type RPCSendOptions,
	logger,
} from "./io.js";

export type RPCSetupOptions<Q, R> = {
	topic: string;
	queryType: AbstractType<Q>;
	responseType: AbstractType<R>;
	responseHandler?: ResponseHandler<Q, R>;
	/**
	 * Opt-in fast path: resolve the request object for an inbound message
	 * without decoding `RequestV0.request` (e.g. from a native wire stash
	 * keyed by the message id). Returning undefined falls back to the normal
	 * decrypt + deserialize path, so semantics never diverge.
	 */
	resolveRequest?: (message: DataMessage) => Q | undefined;
};
export type RequestContext = {
	from?: PublicSignKey;
	message: DataMessage;
	transport: RequestTransportContext;
};
export type ResponseHandler<Q, R> = (
	query: Q,
	context: RequestContext,
) => Promise<R | undefined> | R | undefined;

const createValueResolver = <T>(
	type: AbstractType<T> | Uint8Array,
): ((decryptedThings: DecryptedThing<T>) => T) => {
	if ((type as any) === Uint8Array) {
		return (decrypted) => decrypted._data as T;
	} else {
		return (decrypted) => decrypted.getValue(type as AbstractType<T>);
	}
};

const getExpectedResponders = (
	mode: PubSubPublishOptions["mode"] | undefined,
): Set<string> | undefined => {
	const to = (mode as { to?: unknown } | undefined)?.to;
	if (!Array.isArray(to) || to.length === 0) {
		return undefined;
	}
	const responders = to.filter(
		(value): value is string => typeof value === "string",
	);
	return responders.length > 0 ? new Set(responders) : undefined;
};

export type ResponseEvent<R> = {
	response: R;
	message: DataMessage;
	from?: PublicSignKey;
};

export type RequestEvent<R> = {
	request: R;
	message: DataMessage;
	from?: PublicSignKey;
};

export type CodecErrorEvent = {
	error: Error;
	stage:
		| "decode-request"
		| "handle-request"
		| "encode-response"
		| "publish-response"
		| "dispatch-response"
		| "decode-response";
	message: DataMessage;
};

export interface RPCEvents<Q, R> extends ProgramEvents {
	request: CustomEvent<RequestEvent<Q>>;
	response: CustomEvent<ResponseEvent<R>>;
	codecError: CustomEvent<CodecErrorEvent>;
}

@variant("rpc")
export class RPC<Q, R> extends Program<RPCSetupOptions<Q, R>, RPCEvents<Q, R>> {
	private _subscribed = false;
	private _responseHandler?: ResponseHandler<Q, (R | undefined) | R>;
	private _resolveRequest?: (message: DataMessage) => Q | undefined;
	private _responseResolver!: Map<
		string,
		(properties: { response: ResponseV0; message: DataMessage }) => any
	>;
	private _requestType!: AbstractType<Q> | Uint8ArrayConstructor;
	private _requestTypeIsUint8Array!: boolean;
	private _responseType!: AbstractType<R>;
	private _rpcTopic!: string;
	private _onMessageBinded: ((arg: any) => any) | undefined = undefined;
	private _listenerAttached = false;
	private _keypair!: X25519Keypair;
	private _getResponseValueFn!: (decrypted: DecryptedThing<R>) => R;
	private _getRequestValueFn!: (decrypted: DecryptedThing<Q>) => Q;

	async open(args: RPCSetupOptions<Q, R>): Promise<void> {
		this._rpcTopic = args.topic ?? this._rpcTopic;
		this._responseHandler = args.responseHandler;
		this._resolveRequest = args.resolveRequest;
		this._requestType = args.queryType;
		this._requestTypeIsUint8Array = (this._requestType as any) === Uint8Array;
		this._responseType = args.responseType;
		this._responseResolver = new Map();
		this._getResponseValueFn = createValueResolver(this._responseType);
		this._getRequestValueFn = createValueResolver(this._requestType);

		this._keypair = await X25519Keypair.create();
		await this.subscribe();
	}

	private async _close(): Promise<void> {
		if (this._subscribed) {
			await this.node.services.pubsub.unsubscribe(this.topic);
			this._subscribed = false;
		}
		if (this._listenerAttached) {
			await this.node.services.pubsub.removeEventListener(
				"data",
				this._onMessageBinded,
			);
			this._listenerAttached = false;
		}
	}
	public async close(from?: Program): Promise<boolean> {
		let firstError: unknown;
		let closed = false;
		try {
			closed = await super.close(from);
		} catch (error) {
			if (!this.closed || this.pendingTerminalOperation !== "close") {
				throw error;
			}
			firstError = error;
		}
		if (!closed && firstError === undefined) {
			return false;
		}
		try {
			await this._close();
		} catch (error) {
			firstError ??= error;
		}
		if (firstError !== undefined) {
			throw firstError;
		}
		return true;
	}

	public async drop(from?: Program): Promise<boolean> {
		let firstError: unknown;
		let dropped = false;
		try {
			dropped = await super.drop(from);
		} catch (error) {
			if (!this.closed || this.pendingTerminalOperation !== "drop") {
				throw error;
			}
			firstError = error;
		}
		if (!dropped && firstError === undefined) {
			return false;
		}
		try {
			await this._close();
		} catch (error) {
			firstError ??= error;
		}
		if (firstError !== undefined) {
			throw firstError;
		}
		return true;
	}

	private _subscribing: Promise<void> | void | undefined;
	async subscribe(): Promise<void> {
		await this._subscribing;
		if (this._subscribed) {
			return;
		}

		this._subscribed = true;

		this._onMessageBinded = this._onMessageBinded || this._onMessage.bind(this);

		this.node.services.pubsub.addEventListener("data", this._onMessageBinded!);
		this._listenerAttached = true;

		this._subscribing = this.node.services.pubsub.subscribe(this.topic);

		await this._subscribing;
		logger.trace("subscribing to query topic (responses): " + this.topic);
	}

	private async _onMessage(evt: CustomEvent<DataEvent>): Promise<void> {
		if (this.closed) {
			return;
		}
		const { data, message } = evt.detail;

		if (data?.topics.find((x) => x === this.topic) != null) {
			let rpcMessage: RPCMessage;
			try {
				rpcMessage = deserialize(data.data, RPCMessage);
			} catch (error: any) {
				if (error instanceof BorshError) {
					// The envelope itself is not an RPCMessage: another protocol
					// sharing the topic. This is the only BorshError that is safe
					// to treat as "not for us".
					logger.trace("Got message for a different namespace");
					return;
				}
				logger.error(
					"Error decoding RPC message: " +
						(error?.message ? error?.message?.toString() : error),
				);
				return;
			}
			let stage: CodecErrorEvent["stage"] = "decode-request";
			try {
				if (rpcMessage instanceof RequestV0) {
					if (this._responseHandler) {
						let request: Q | undefined;
						try {
							request = this._resolveRequest?.(message);
						} catch (error) {
							// A throwing resolve hook must fall back to the
							// normal decode path (the documented contract:
							// "semantics never diverge"), not drop the message.
							logger.error(
								"resolveRequest hook threw; falling back to decode: " +
									(error instanceof Error ? error.message : String(error)),
							);
							request = undefined;
						}
						if (request === undefined) {
							const maybeEncrypted = rpcMessage.request;
							const decrypted = await maybeEncrypted.decrypt(
								this.node.services.keychain,
							);
							request = this._getRequestValueFn(decrypted);
						}
						if (this.closed) {
							return;
						}
						stage = "handle-request";
						let from = message.header.signatures!.publicKeys[0];

						this.events.dispatchEvent(
							new CustomEvent("request", {
								// TODO should this event also be emitted if the responseHandler is omitted?
								detail: {
									request,
									message: message,
									from: from,
								},
							}),
						);

						const transport = createRequestTransportContext(message);
						const response = await this._responseHandler(request, {
							from,
							message: message,
							transport,
						});

						if (!this.closed && response && rpcMessage.respondTo) {
							// send query and wait for replies in a generator like behaviour
							stage = "encode-response";
							const serializedResponse = serialize(response);
							stage = "publish-response";

							// we use the peerId/libp2p identity for signatures, since we want to be able to send a message
							// with pubsub with a certain receiver. If we use (this.identity) we are going to use an identity
							// that is now known in the .pubsub network, hence the message might not be delivired if we
							// send with { to: [RECIEVER] } param

							const decryptedMessage = new DecryptedThing<Uint8Array>({
								data: serializedResponse,
							});
							let maybeEncryptedMessage: MaybeEncrypted<Uint8Array> =
								decryptedMessage;

							maybeEncryptedMessage = await decryptedMessage.encrypt(
								this._keypair,
								[rpcMessage.respondTo],
							);
							if (this.closed) {
								return;
							}

							await this.node.services.pubsub.publish(
								serialize(
									new ResponseV0({
										response: maybeEncryptedMessage,
										requestId: message.id,
									}),
								),
								transport.withResponseOptions({
									topics: [this.topic],

									/// TODO make redundancy parameter configurable?
									mode: new SilentDelivery({
										to: [message.header.signatures!.publicKeys[0]],
										redundancy: 1,
									}),
								}),
							);
						}
					}
				} else if (rpcMessage instanceof ResponseV0) {
					stage = "dispatch-response";
					const id = toBase64(rpcMessage.requestId);
					const handler = this._responseResolver.get(id);
					// TODO evaluate when and how handler can be missing
					handler?.({
						message,
						response: rpcMessage,
					});
				}
			} catch (error: any) {
				if (error instanceof AccessError) {
					logger.trace("Got message I could not decrypt");
					return;
				}

				if (error instanceof BorshError) {
					// Past the envelope decode this is OUR payload failing to
					// (de)serialize — e.g. a response embedding entries that
					// cannot be re-serialized. Swallowing it silently makes the
					// requester time out with no trace, so surface it.
					logger.error(
						"RPC serialization failed at stage " +
							stage +
							": " +
							error.message,
					);
					this.events.dispatchEvent(
						new CustomEvent<CodecErrorEvent>("codecError", {
							detail: { error, stage, message },
						}),
					);
					return;
				}

				logger.error(
					"Error handling query: " +
						(error?.message ? error?.message?.toString() : error),
				);
			}
		}
	}

	private async seal(
		request: Q,
		respondTo?: X25519PublicKey,
		options?: EncryptionOptions,
	) {
		const requestData = this._requestTypeIsUint8Array
			? (request as Uint8Array)
			: serialize(request);

		const decryptedMessage = new DecryptedThing<Uint8Array>({
			data: requestData,
		});

		let maybeEncryptedMessage: MaybeEncrypted<Uint8Array> = decryptedMessage;

		if (
			options?.encryption?.responders &&
			options?.encryption?.responders.length > 0
		) {
			maybeEncryptedMessage = await decryptedMessage.encrypt(
				options.encryption.key,
				options.encryption.responders,
			);
		}

		const requestMessage = new RequestV0({
			request: maybeEncryptedMessage,
			respondTo,
		});

		return requestMessage;
	}

	private getPublishOptions(
		id?: Uint8Array,
		options?: RPCSendOptions,
		signal?: AbortSignal,
	): PubSubPublishOptions {
		const explicitMode =
			options && "mode" in options ? options.mode : undefined;
		const explicitTo = options && "to" in options ? options.to : undefined;
		const normalizedMode =
			explicitMode ??
			(explicitTo != null
				? new SilentDelivery({
						to: explicitTo,
						redundancy: 1,
					})
				: undefined);
		return {
			id,
			priority: options?.priority,
			responsePriority: options?.responsePriority,
			expiresAt: options?.expiresAt,
			mode: normalizedMode,
			topics: [this.topic],
			extraSigners: options?.extraSigners,
			signal,
		};
	}

	/**
	 * Send message and don't expect any response
	 * @param message
	 * @param options
	 */
	public async send(
		message: Q,
		options?: RPCSendOptions,
	): Promise<void> {
		await this.node.services.pubsub.publish(
			serialize(await this.seal(message, undefined, options)),
			this.getPublishOptions(undefined, options, options?.signal),
		);
	}

	private handleDecodedResponse(
		response: {
			response: R;
			message: DataMessage;
			from?: PublicSignKey;
		},
		promise: DeferredPromise<any>,
		allResults: RPCResponse<R>[],
		responders: Set<string>,
		expectedResponders?: Set<string>,
		options?: RPCRequestOptions<R>,
	) {
		const expectedResponseHash =
			expectedResponders && response.from
				? response.from.hashcode()
				: undefined;
		if (
			expectedResponseHash &&
			expectedResponders.has(expectedResponseHash) &&
			responders.has(expectedResponseHash)
		) {
			return;
		}

		this.events.dispatchEvent(
			new CustomEvent("response", {
				detail: response,
			}),
		);

		if (expectedResponders) {
			if (response.from && expectedResponders?.has(response.from.hashcode())) {
				options?.onResponse &&
					options?.onResponse(response.response, response.from);
				allResults.push(response);

				responders.add(response.from.hashcode());
				if (responders.size === expectedResponders.size) {
					promise.resolve();
				}
			}
		} else {
			options?.onResponse &&
				options?.onResponse(response.response, response.from);
			allResults.push(response);
			if (
				options?.amount != null &&
				allResults.length >= (options?.amount as number)
			) {
				promise.resolve();
			}
		}
	}
	private createResponseHandler(
		promise: DeferredPromise<any>,
		keypair: X25519Keypair,
		allResults: RPCResponse<R>[],
		responders: Set<string>,
		expectedResponders?: Set<string>,
		options?: RPCRequestOptions<R>,
	) {
		return async (properties: {
			response: ResponseV0;
			message: DataMessage;
		}) => {
			let decoded = false;
			try {
				const { response, message } = properties;
				const from = message.header.signatures!.publicKeys[0];

				if (options?.isTrusted && !(await options?.isTrusted(from))) {
					return;
				}

				const maybeEncrypted = response.response;
				const decrypted = await maybeEncrypted.decrypt(keypair);
				const resultData = this._getResponseValueFn(decrypted);
				decoded = true;

				this.handleDecodedResponse(
					{
						response: resultData,
						message: message,
						from: from,
					},
					promise,
					allResults,
					responders,
					expectedResponders,
					options,
				);
			} catch (error: any) {
				if (error instanceof AccessError) {
					return; // Ignore things we can not open
				}

				if (error instanceof BorshError) {
					if (decoded) {
						// The response decoded fine; the error came from a
						// consumer callback afterwards. Don't misreport it as
						// a codec failure (control flow matches other swallows).
						logger.error(
							"Error handling decoded RPC response: " + error.message,
						);
						return;
					}
					// The response was addressed to us (encrypted to this
					// request's keypair) but its payload failed to decode —
					// that is a codec bug, not a namespace conflict. Keep the
					// request alive for other responders, but surface it.
					logger.error(
						"Failed to decode RPC response: " + error.message,
					);
					this.events.dispatchEvent(
						new CustomEvent<CodecErrorEvent>("codecError", {
							detail: {
								error,
								stage: "decode-response",
								message: properties.message,
							},
						}),
					);
					return;
				}

				logger.error("failed ot deserialize query response: " + error?.message);
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
		options?: RPCRequestOptions<R>,
	): Promise<RPCResponse<R>[]> {
		const requestSignal = options?.signal;
		const getAbortReason = (signal?: AbortSignal): unknown =>
			signal?.reason === undefined ? new AbortError() : signal.reason;
		if (requestSignal?.aborted) {
			throw getAbortReason(requestSignal);
		}

		let rejectSetupForAbort!: (reason?: unknown) => void;
		const setupAbortPromise = new Promise<never>((_resolve, reject) => {
			rejectSetupForAbort = reject;
		});
		const setupAbortListener = () => {
			rejectSetupForAbort(getAbortReason(requestSignal));
		};
		requestSignal?.addEventListener("abort", setupAbortListener, { once: true });
		const setupPromise = (async () => {
			// We are generatinga new encryption keypair for each send, so we now that when we get the responses, they are encrypted specifcally for me, and for this request
			// this allows us to easily disregard a bunch of message just beacuse they are for a different receiver!
			const keypair = await X25519Keypair.create();
			if (requestSignal?.aborted) {
				throw getAbortReason(requestSignal);
			}
			const requestMessage = await this.seal(
				request,
				keypair.publicKey,
				options,
			);
			return { keypair, requestBytes: serialize(requestMessage) };
		})();
		let setup: Awaited<typeof setupPromise>;
		try {
			setup = await Promise.race([setupPromise, setupAbortPromise]);
		} finally {
			requestSignal?.removeEventListener("abort", setupAbortListener);
		}
		const { keypair, requestBytes } = setup;
		if (requestSignal?.aborted) {
			throw getAbortReason(requestSignal);
		}

		const allResults: RPCResponse<R>[] = [];

		const deferredPromise = pDefer<void>();
		const publishAbortController = new AbortController();
		const abortPublish = (reason: unknown) => {
			if (!publishAbortController.signal.aborted) {
				publishAbortController.abort(reason);
			}
		};

		if (this.closed) {
			throw new AbortError("Closed");
		}
		const timeoutFn = setTimeout(
			() => {
				abortPublish(new AbortError("RPC request timeout"));
				deferredPromise.resolve();
			},
			options?.timeout || 10 * 1000,
		);

		const rejectForAbort = (signal?: AbortSignal) => {
			const reason = getAbortReason(signal);
			abortPublish(reason);
			deferredPromise.reject(reason);
		};
		const abortListener = (event: Event) => {
			rejectForAbort(event.target as AbortSignal | undefined);
		};
		requestSignal?.addEventListener("abort", abortListener);
		// Cover the narrow handoff between the setup listener and the active request
		// listener before registering any resolver or transport side effects.
		if (requestSignal?.aborted) {
			const reason = getAbortReason(requestSignal);
			abortPublish(reason);
			clearTimeout(timeoutFn);
			requestSignal.removeEventListener("abort", abortListener);
			throw reason;
		}

		const closeListener = () => {
			const reason = new AbortError("Closed");
			abortPublish(reason);
			deferredPromise.reject(reason);
		};
		const dropListener = () => {
			const reason = new AbortError("Dropped");
			abortPublish(reason);
			deferredPromise.reject(reason);
		};

		this.events.addEventListener("close", closeListener);
		this.events.addEventListener("drop", dropListener);

		const expectedResponders = getExpectedResponders(options?.mode);

		const responders = new Set<string>();

		const messageId = randomBytes(32);
		const id = toBase64(messageId);

		const responseHandler = this.createResponseHandler(
			deferredPromise,
			keypair,
			allResults,
			responders,
			expectedResponders,
			options,
		);
		this._responseResolver.set(id, responseHandler);
		void deferredPromise.promise
			.finally(() => {
				abortPublish(new AbortError("Resolved early"));
			})
			.catch(() => {});

		try {
			if (requestSignal?.aborted) {
				throw getAbortReason(requestSignal);
			}
			if (options?.responseInterceptor) {
				options.responseInterceptor((response: RPCResponse<R>) => {
					return this.handleDecodedResponse(
						response,
						deferredPromise,
						allResults,
						responders,
						expectedResponders,
						options,
					);
				});
			}
			if (requestSignal?.aborted) {
				throw getAbortReason(requestSignal);
			}
			const publishPromise = Promise.resolve()
				.then(() =>
					this.node.services.pubsub.publish(
						requestBytes,
						this.getPublishOptions(
							messageId,
							options,
							publishAbortController.signal,
						),
					),
				)
				.catch((error: any) => {
					if (
						publishAbortController.signal.aborted &&
						(error instanceof AbortError ||
							error === publishAbortController.signal.reason)
					) {
						return;
					}
					if (error instanceof TimeoutError) {
						// Ignore publish timeouts
						return;
					}
					throw error;
				});
			await Promise.race([publishPromise, deferredPromise.promise]);
			await deferredPromise.promise;
		} finally {
			abortPublish(new AbortError("RPC request finished"));
			clearTimeout(timeoutFn);
			this.events.removeEventListener("close", closeListener);
			this.events.removeEventListener("drop", dropListener);
			requestSignal?.removeEventListener("abort", abortListener);
			this._responseResolver.delete(id);
		}

		return allResults;
	}

	public get topic(): string {
		if (!this._rpcTopic) {
			throw new Error("Not initialized");
		}
		return this._rpcTopic;
	}

	getTopics(): string[] {
		return [this.topic];
	}
}
