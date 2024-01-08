import {
	X25519PublicKey,
	Ed25519PublicKey,
	PublicSignKey,
	X25519Keypair
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import { DeliveryMode } from "@peerbit/stream-interface";

export const logger = loggerFn({ module: "rpc" });
export type RPCOptions<R> = {
	amount?: number;
	timeout?: number;
	isTrusted?: (publicKey: PublicSignKey) => Promise<boolean>;
	onResponse?: (response: R, from?: PublicSignKey) => void;
	stopper?: (stopper: () => void) => void;
} & PublishOptions;

export type PublishOptions = {
	encryption?: {
		key: X25519Keypair;
		responders?: (X25519PublicKey | Ed25519PublicKey)[];
	};
	strict?: boolean;
	mode?: DeliveryMode;
};

export type RPCResponse<R> = { response: R; from?: PublicSignKey };
