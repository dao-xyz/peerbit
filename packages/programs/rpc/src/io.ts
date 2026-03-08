import type {
	Ed25519PublicKey,
	PublicSignKey,
	X25519Keypair,
	X25519PublicKey,
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import type {
	DataMessage,
	ExpiresAtOptions,
	PriorityOptions,
	ResponsePriorityOptions,
	WithExtraSigners,
	WithMode,
	WithTo,
} from "@peerbit/stream-interface";

export const logger = loggerFn("peerbit:rpc");
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
	ResponsePriorityOptions &
	ExpiresAtOptions &
	WithExtraSigners &
	RequestResponseInterceptor<R>;

export type RPCSendOptions = EncryptionOptions &
	(WithMode | WithTo) &
	PriorityOptions &
	ResponsePriorityOptions &
	ExpiresAtOptions &
	WithExtraSigners & {
		signal?: AbortSignal;
	};

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
