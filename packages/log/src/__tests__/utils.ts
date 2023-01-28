import path from "path";
import { AbstractLevel } from "abstract-level";
import { Level } from "level";
import { KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { Identity } from "../identity";

export const createStore = async (
	path = "./tmp/log/keystore"
): Promise<AbstractLevel<any, string, Uint8Array>> => {
	const fs = await import("fs");
	if (fs && fs.mkdirSync) {
		fs.mkdirSync(path, { recursive: true });
	}
	return new Level(path, { valueEncoding: "view" });
};
export const signingKeysFixturesPath = (dir: string) =>
	path.join(dir, "./fixtures/keys/signing-keys");
export const testKeyStorePath = (dir: string) =>
	path.join("./tmp/keys/signing-keys", dir);

export const identityFromSignKey = (
	key: KeyWithMeta<Ed25519Keypair>
): Identity => {
	if (!key) {
		throw new Error("Key not defined");
	}
	return {
		...key.keypair,
		sign: async (data: Uint8Array) => key.keypair.sign(data),
	};
};
