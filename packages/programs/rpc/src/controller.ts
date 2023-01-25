import {
	AbstractType,
	BorshError,
	deserialize,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { AccessError, decryptVerifyInto } from "@dao-xyz/peerbit-crypto";
import { RequestV0, ResponseV0, RPCMessage } from "./encoding.js";
import { send, RPCOptions, respond, logger } from "./io.js";
import {
	AbstractProgram,
	Address,
	ComposableProgram,
	ProgramInitializationOptions,
} from "@dao-xyz/peerbit-program";
import { Identity } from "@dao-xyz/peerbit-log";
import { X25519Keypair } from "@dao-xyz/peerbit-crypto";
import { PubSubData } from "@dao-xyz/libp2p-direct-sub";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";

export type SearchContext = (() => Address) | AbstractProgram | string;
export type CanRead = (key?: PublicSignKey) => Promise<boolean> | boolean;
/* export type RPCTopicOption =
	| { queryAddressSuffix: string }
	| { rpcRegion: string }; */
export type RPCSetupOptions<Q, R> = {
	topic?: string;
	queryType: AbstractType<Q>;
	responseType: AbstractType<R>;
	canRead?: CanRead;
	context: SearchContext;
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
/* 
export abstract class RPCTopic {
	abstract from(address: Address): string;
}

@variant(0)
export class RPCRegion extends RPCTopic {
	@field({ type: "string" })
	id: string;

	constructor(properties?: { id: string }) {
		super();
		if (properties) {
			this.id = properties.id;
		}
	}

	from(_address: Address) {
		return this.id;
	}
}

@variant(1)
export class RPCAddressSuffix extends RPCTopic {
	@field({ type: "string" })
	suffix: string;
	constructor(properties?: { suffix: string }) {
		super();
		if (properties) {§
			this.suffix = properties.suffix;
		}
	}

	from(address: Address) {
		return address.toString() + "/" + this.suffix;
	}
}
 */
@variant("rpc")
export class RPC<Q, R> extends ComposableProgram {
	/* rpcRegion?: RPCTopic; */
	canRead: CanRead;

	_subscribed = false;
	_onMessageBinded: (evt: CustomEvent<PubSubData>) => any;
	_responseHandler: ResponseHandler<Q, (R | undefined) | R>;
	_requestType: AbstractType<Q>;
	_responseType: AbstractType<R>;
	_rpcTopic: string | undefined;
	_context: SearchContext;

	public setup(options: RPCSetupOptions<Q, R>) {
		/* if (options.rpcTopic) {
			if (
				!!(options.rpcTopic as { rpcRegion }).rpcRegion &&
				!!(options.rpcTopic as { rpcRegion }).rpcRegion ==
				!!(options.rpcTopic as { queryAddressSuffix })
					.queryAddressSuffix
			) {
				throw new Error(
					"Expected either rpcRegion or queryAddressSuffix or none"
				);
			}
			if ((options.rpcTopic as { rpcRegion }).rpcRegion) {
				this.rpcRegion = new RPCRegion({
					id: (options.rpcTopic as { rpcRegion }).rpcRegion,
				});
			} else if (options.rpcTopic as { queryAddressSuffix }) {
				this.rpcRegion = new RPCAddressSuffix({
					suffix: (options.rpcTopic as { queryAddressSuffix })
						.queryAddressSuffix,
				});
			}
		} */
		this._rpcTopic = options.topic ?? this._rpcTopic;
		this._context = options.context;
		this._responseHandler = options.responseHandler;
		this._requestType = options.queryType;
		this._responseType = options.responseType;
		this.canRead = options.canRead || (() => Promise.resolve(true));
	}

	async init(
		libp2p: Libp2pExtended,
		identity: Identity,
		options: ProgramInitializationOptions
	): Promise<this> {
		await super.init(libp2p, identity, options);
		this._rpcTopic = this._rpcTopic || this.topic;
		if (options.replicate) {
			this._subscribe();
		}
		return this;
	}

	public async close(): Promise<void> {
		await this._initializationPromise;
		if (this._subscribed) {
			await this._libp2p.directsub.unsubscribe(
				this.rpcTopic
			);
			await this._libp2p.directsub.removeEventListener(
				"data",
				this._onMessageBinded
			);
			this._subscribed = false;
		}
	}

	_subscribe(): void {
		if (this._subscribed) {
			return;
		}

		this._onMessageBinded = this._onMessage.bind(this);
		this._libp2p.directsub.subscribe(this.rpcTopic);
		this._libp2p.directsub.addEventListener("data", this._onMessageBinded);
		logger.debug("subscribing to query topic: " + this.rpcTopic);
		this._subscribed = true;
	}

	async _onMessage(evt: CustomEvent<PubSubData>): Promise<void> {
		//if (evt.detail.type === "signed")
		{
			const message = evt.detail;
			if (message) {
				/*  if (message.from.equals(this._libp2p.peerId)) {
					 return;
				 } */
				try {
					try {
						const { result: request, from } =
							await decryptVerifyInto(
								message.data,
								RPCMessage,
								this._encryption?.getAnyKeypair ||
								(() => Promise.resolve(undefined)),
								{
									isTrusted: (key) =>
										Promise.resolve(
											this.canRead(
												key.signature?.publicKey
											)
										),
								}
							);
						if (request instanceof RequestV0) {
							if (request.context != undefined) {
								if (request.context != this.contextAddress) {
									logger.debug(
										"Recieved a request for another context"
									);
									return;
								}
							}

							const response = await this._responseHandler(
								(this._requestType as any) === Uint8Array
									? (request.request as Q)
									: deserialize(
										request.request,
										this._requestType
									),
								{
									address: this.contextAddress,
									from,
								}
							);

							if (response) {
								await respond(
									this._libp2p,
									this.getRpcResponseTopic(request),
									request,
									new ResponseV0({
										response: serialize(response),
										context: this.contextAddress,
									}),
									{
										encryption: this._encryption,
										signer: this._identity,
									}
								);
							}
						} else {
							return;
						}
					} catch (error: any) {
						if (error instanceof BorshError) {
							logger.debug(
								"Got message for a different namespace"
							);
							return;
						}
						if (error instanceof AccessError) {
							logger.debug("Got message I could not decrypt");
							return;
						}

						logger.error(
							"Error handling query: " +
							(error?.message
								? error?.message?.toString()
								: error)
						);
						throw error;
					}
				} catch (error: any) {
					if (error.constructor.name === "BorshError") {
						return; // unknown message
					}
					console.error(error);
				}
			}
		}
	}

	public async send(
		request: Q,
		responseHandler: (response: R, from?: PublicSignKey) => void,
		options?: RPCOptions
	): Promise<void> {
		// We are generatinga new encryption keypair for each send, so we now that when we get the responses, they are encrypted specifcally for me, and for this request
		// this allows us to easily disregard a bunch of message just beacuse they are for a different reciever!
		const keypair = await X25519Keypair.create();
		const r = new RequestV0({
			request:
				(this._requestType as any) === Uint8Array
					? (request as Uint8Array)
					: serialize(request),
			context: options?.context || this.contextAddress.toString(),
			respondTo: keypair.publicKey,
		});
		return send(
			this._libp2p,
			this.rpcTopic,
			this.getRpcResponseTopic(r),
			r,
			(response, from) => {
				responseHandler(
					deserialize(response.response, this._responseType),
					from
				);
			},
			keypair,
			options
		);
	}

	get contextAddress(): string {
		if (typeof this._context === "string") {
			return this._context;
		}
		return this._context instanceof AbstractProgram
			? this._context.address.toString()
			: this._context().toString();
	}

	public get rpcTopic(): string {
		if (!this._rpcTopic) {
			throw new Error("Not initialized");
		}
		/* 	if (this.rpcRegion) {
				if (!this.parentProgram.address) {
					throw new Error("Not initialized");
				}
				return this.rpcRegion.from(this.parentProgram.address);
			} */
		return this._rpcTopic;
	}

	public getRpcResponseTopic(_request: RequestV0): string {
		return this.rpcTopic;
	}
}
