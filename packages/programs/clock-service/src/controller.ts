import { variant } from "@dao-xyz/borsh";
import { RPC, RPCOptions } from "@dao-xyz/peerbit-rpc";
import { Program } from "@dao-xyz/peerbit-program";
import pino from "pino";
import {
    DecryptedThing,
    MaybeEncrypted,
    SignatureWithKey,
} from "@dao-xyz/peerbit-crypto";
import { Entry, HLC, Signatures } from "@dao-xyz/ipfs-log";
import { field, serialize, deserialize } from "@dao-xyz/borsh";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";

const logger = pino().child({ module: "remote_signer" });

const abs = (n) => (n < 0n ? -n : n);

@variant("clock_service")
export class ClockService extends Program {
    @field({ type: RPC })
    _remoteSigner: RPC<Uint8Array, SignatureWithKey>;

    @field({ type: TrustedNetwork })
    _trustedNetwork: TrustedNetwork;

    _hlc: HLC = new HLC();
    _maxError = 10e9; // 10 seconds

    constructor(properties?: {
        trustedNetwork: TrustedNetwork;
        remoteSigner?: RPC<Uint8Array, SignatureWithKey>;
    }) {
        super();
        if (properties) {
            this._remoteSigner = properties.remoteSigner || new RPC();
            this._trustedNetwork = properties.trustedNetwork;
        }
    }

    async setup() {
        await this._trustedNetwork.setup();
        await this._remoteSigner.setup({
            context: this,
            queryType: Uint8Array,
            responseType: SignatureWithKey,
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
                    return;
                }
                const signature = await this._identity.sign(entry.toSignable());
                return new SignatureWithKey({
                    publicKey: this._identity.publicKey,
                    signature,
                });
            },
        });
    }

    async sign(data: Uint8Array): Promise<SignatureWithKey> {
        const signatures: SignatureWithKey[] = [];
        await this._remoteSigner.send(
            data,
            (response) => {
                signatures.push(response);
            },
            { waitForAmount: 1 }
        );
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
