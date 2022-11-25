import { variant, option } from "@dao-xyz/borsh";
import { RPC } from "@dao-xyz/peerbit-rpc";
import { Program } from "@dao-xyz/peerbit-program";
import { SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { Entry, HLC } from "@dao-xyz/ipfs-log";
import { field, deserialize } from "@dao-xyz/borsh";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";

import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
const logger = loggerFn({ module: "reomte-signer" });

const abs = (n) => (n < 0n ? -n : n);

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

@variant("clock_service")
export class ClockService extends Program {
    @field({ type: RPC })
    _remoteSigner: RPC<Uint8Array, Ok | SignError>;

    @field({ type: TrustedNetwork })
    _trustedNetwork: TrustedNetwork;

    _hlc: HLC = new HLC();
    _maxError: bigint; // 10 seconds

    constructor(properties?: {
        trustedNetwork: TrustedNetwork;
        remoteSigner?: RPC<Uint8Array, Result>;
    }) {
        super();
        if (properties) {
            this._remoteSigner = properties.remoteSigner || new RPC();
            this._trustedNetwork = properties.trustedNetwork;
        }
    }

    /**
     * @param maxError, in ms, defaults to 10 seconds
     */
    async setup(properties?: { maxTimeError: number }) {
        this._maxError = BigInt((properties?.maxTimeError || 10e3) * 1e6);
        await this._trustedNetwork.setup();
        await this._remoteSigner.setup({
            context: this,
            queryType: Uint8Array,
            responseType: Result,
            responseHandler: async (arr, context) => {
                const entry = deserialize(arr, Entry);
                if (entry.hash) {
                    logger.warn("Recieved entry with hash, unexpected");
                }

                entry._signatures = undefined; // because we dont want to sign signatures

                const now = this._hlc.now().wallTime;
                const cmp = (await entry.getClock()).timestamp.wallTime;
                if (abs(now - cmp) > this._maxError) {
                    logger.info("Recieved an entry with an invalid timestamp");
                    return new SignError({
                        message: "Recieved an entry with an invalid timestamp",
                    });
                }
                const signature = await this._identity.sign(entry.toSignable());
                return new Ok({
                    signature: new SignatureWithKey({
                        publicKey: this._identity.publicKey,
                        signature,
                    }),
                });
            },
        });
    }

    async sign(data: Uint8Array): Promise<SignatureWithKey> {
        const signatures: SignatureWithKey[] = [];
        let error: Error | undefined = undefined;
        await this._remoteSigner.send(
            data,
            (response) => {
                if (response instanceof Ok) {
                    signatures.push(response.signature);
                } else {
                    error = new Error(response.message);
                }
            },
            { waitForAmount: 1 }
        );
        if (error) {
            throw error;
        }
        return signatures[0];
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
