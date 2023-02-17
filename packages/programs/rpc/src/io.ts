import {
	MaybeSigned,
	X25519PublicKey,
	Ed25519PublicKey,
	GetEncryptionKeypair,
	PublicSignKey,
} from "@dao-xyz/peerbit-crypto";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";

export const logger = loggerFn({ module: "rpc" });
export type RPCOptions<R> = {
	encryption?: {
		key: GetEncryptionKeypair;
		responders?: (X25519PublicKey | Ed25519PublicKey)[];
	};
	amount?: number;
	timeout?: number;
	isTrusted?: (publicKey: MaybeSigned<any>) => Promise<boolean>;
	responseRecievers?: X25519PublicKey[];
	strict?: boolean;
	to?: PublicSignKey[] | string[];
	onResponse?: (response: R, from?: PublicSignKey) => void;
	stopper?: (stopper: () => void) => void;
};

export type RPCResponse<R> = { response: R; from?: PublicSignKey };
