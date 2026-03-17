import crypto from "crypto";

export interface IncrementalSHA256 {
	update(bytes: Uint8Array): IncrementalSHA256;
	digest(): Uint8Array;
}

class NodeIncrementalSHA256 implements IncrementalSHA256 {
	private readonly hash = crypto.createHash("sha256");

	update(bytes: Uint8Array): IncrementalSHA256 {
		this.hash.update(bytes);
		return this;
	}

	digest(): Uint8Array {
		return this.hash.digest();
	}
}

export const createSHA256 = (): IncrementalSHA256 =>
	new NodeIncrementalSHA256();

export const sha256Base64Sync = (bytes: Uint8Array): string =>
	crypto.createHash("sha256").update(bytes).digest("base64");
export const sha256Base64 = async (bytes: Uint8Array): Promise<string> =>
	crypto.createHash("sha256").update(bytes).digest("base64");
export const sha256 = async (bytes: Uint8Array): Promise<Uint8Array> =>
	crypto.createHash("sha256").update(bytes).digest();
export const sha256Sync = (bytes: Uint8Array): Uint8Array =>
	crypto.createHash("sha256").update(bytes).digest();
