import type {
	Ed25519PublicKey,
	PublicSignKey,
	X25519Keypair,
	X25519PublicKey,
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import type {
	DataMessage,
	PriorityOptions,
	WithExtraSigners,
	WithMode,
} from "@peerbit/stream-interface";

export const logger: ReturnType<typeof loggerFn> = loggerFn({ module: "rpc" });
export type RPCRequestResponseOptions<R> = {
	amount?: number;
	timeout?: number;
	isTrusted?: (publicKey: PublicSignKey) => Promise<boolean>;
	onResponse?: (response: R, from?: PublicSignKey) => void;
	signal?: AbortSignal;
};

export type RequestResponseInterceptor<R> = {
	responseInterceptor?: (fn: (response: RPCResponse<R>) => void) => void;
};

export type RPCRequestOptions<R> = RPCRequestResponseOptions<R> &
	EncryptionOptions &
	WithMode &
	PriorityOptions &
	WithExtraSigners &
	RequestResponseInterceptor<R>;

export type EncryptionOptions = {
	encryption?: {
		key: X25519Keypair;
		responders?: (X25519PublicKey | Ed25519PublicKey)[];
	};
};

export type RPCResponse<R> = {
	response: R;
	message: DataMessage;
	from?: PublicSignKey;
};
