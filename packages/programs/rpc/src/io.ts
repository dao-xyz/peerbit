import {
	X25519PublicKey,
	Ed25519PublicKey,
	PublicSignKey,
	X25519Keypair
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import { DeliveryMode } from "@peerbit/stream-interface";

export const logger = loggerFn({ module: "rpc" });
export type RPCRequestResponseOptions<R> = {
	amount?: number;
	timeout?: number;
	isTrusted?: (publicKey: PublicSignKey) => Promise<boolean>;
	onResponse?: (response: R, from?: PublicSignKey) => void;
	signal?: AbortSignal;
};

export type RPCRequestOptions<R> = RPCRequestResponseOptions<R> &
	EncryptionOptions &
	WithMode;
export type WithMode = { mode?: DeliveryMode };
export type EncryptionOptions = {
	encryption?: {
		key: X25519Keypair;
		responders?: (X25519PublicKey | Ed25519PublicKey)[];
	};
};

export type RPCResponse<R> = { response: R; from?: PublicSignKey };
