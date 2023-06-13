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
	amount?: number;
	timeout?: number;
	isTrusted?: (publicKey: MaybeSigned<any>) => Promise<boolean>;
	strict?: boolean;
	onResponse?: (response: R, from?: PublicSignKey) => void;
	stopper?: (stopper: () => void) => void;
} & PublishOptions;

export type PublishOptions = {
	encryption?: {
		key: GetEncryptionKeypair;
		responders?: (X25519PublicKey | Ed25519PublicKey)[];
	};
	to?: PublicSignKey[] | string[];
	strict?: boolean;
};

export type RPCResponse<R> = { response: R; from?: PublicSignKey };
