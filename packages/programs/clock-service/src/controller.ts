import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { SignatureWithKey } from "@peerbit/crypto";
import { Entry, HLC } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { Program } from "@peerbit/program";
import { RPC, type RPCResponse } from "@peerbit/rpc";
import { type ReplicationOptions } from "@peerbit/shared-log";
import { TrustedNetwork } from "@peerbit/trusted-network";

const logger = loggerFn("peerbit:program:clock-service");
const warn = logger.newScope("warn");
const abs = (n: number | bigint) => (n < 0n ? -n : n);

export abstract class Result {}

@variant(0)
export class Ok extends Result {
	@field({ type: SignatureWithKey })
	signature: SignatureWithKey;

	constructor(properties?: { signature: SignatureWithKey }) {
		super();
		if (properties) {
			this.signature = properties.signature;
		}
	}
}

@variant(1)
export class SignError extends Result {
	@field({ type: option("string") })
	message?: string;

	constructor(properties?: { message?: string }) {
		super();
		if (properties) {
			this.message = properties.message;
		}
	}
}

type Args = { replicate?: ReplicationOptions; maxTimeError?: number };

@variant("clock_service")
export class ClockService extends Program<Args> {
	@field({ type: RPC })
	private _remoteSigner: RPC<Uint8Array, Ok | SignError>;

	@field({ type: TrustedNetwork })
	private _trustedNetwork: TrustedNetwork;

	private _hlc: HLC = new HLC();
	maxError: bigint; // 10 seconds

	constructor(properties: {
		trustedNetwork: TrustedNetwork;
		remoteSigner?: RPC<Uint8Array, Result>;
	}) {
		super();
		this._remoteSigner = properties.remoteSigner || new RPC();
		this._trustedNetwork = properties.trustedNetwork;
	}

	/**
	 * @param properties in ms, defaults to 10 seconds
	 */
	async open(properties?: Args) {
		if (properties?.replicate === true) {
			properties.replicate = {
				factor: 1,
			};
		}
		if (
			properties?.replicate &&
			(properties.replicate as any)?.["factor"] !== 1
		) {
			throw new Error("ClockService can only be used with a factor of 1");
		}

		this.maxError = BigInt((properties?.maxTimeError || 10e3) * 1e6);
		await this._trustedNetwork.open({ replicate: properties?.replicate });
		const topicBase =
			this._trustedNetwork!.trustGraph!.log.log.idString ?? "clock";
		await this._remoteSigner.open({
			topic: topicBase + "/clock", // TODO do better
			queryType: Uint8Array,
			responseType: Result,
			responseHandler:
				!properties?.replicate /* ||  properties?.replicate instanceof Replicator */ // ?? this feels wrong since the replication duties are dynamic
					? async (arr, context) => {
							const entry = deserialize(arr, Entry);
							if (entry.hash) {
								warn("Recieved entry with hash, unexpected");
							}
							const now = this._hlc.now().wallTime;
							const cmp = (await entry.getClock()).timestamp.wallTime;
							if (abs(now - cmp) > this.maxError) {
								logger("Recieved an entry with an invalid timestamp");
								return new SignError({
									message: "Recieved an entry with an invalid timestamp",
								});
							}
							const signature = await this.node.identity.sign(
								serialize(entry.toSignable()),
							);
							return new Ok({
								signature,
							});
						}
					: undefined,
		});
	}

	async sign(data: Uint8Array): Promise<SignatureWithKey> {
		const responses: RPCResponse<Ok | SignError>[] =
			await this._remoteSigner.request(data, { amount: 1 });

		if (responses.length === 0) {
			throw new Error("Failed to retrieve signatures");
		}
		for (const response of responses) {
			if (response.response instanceof SignError) {
				throw new Error(response.response.message);
			}
		}

		return (responses[0].response as Ok).signature;
	}

	async verify(entry: Entry<any>): Promise<boolean> {
		const signatures = await entry.getSignatures();
		for (const signature of signatures) {
			if (await this._trustedNetwork.isTrusted(signature.publicKey)) {
				return true;
			}
		}
		return false;
	}
}
