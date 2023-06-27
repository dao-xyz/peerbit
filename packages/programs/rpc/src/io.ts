import {
	X25519PublicKey,
	Ed25519PublicKey,
	PublicSignKey,
	X25519Keypair,
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";

export const logger = loggerFn({ module: "rpc" });
export type RPCOptions<R> = {
	amount?: number;
	timeout?: number;
	isTrusted?: (publicKey: PublicSignKey) => Promise<boolean>;
	strict?: boolean;
	onResponse?: (response: R, from?: PublicSignKey) => void;
	stopper?: (stopper: () => void) => void;
} & PublishOptions;

export type PublishOptions = {
	encryption?: {
		key: X25519Keypair;
		responders?: (X25519PublicKey | Ed25519PublicKey)[];
	};
	to?: PublicSignKey[] | string[];
	strict?: boolean;
};

export type RPCResponse<R> = { response: R; from?: PublicSignKey };
