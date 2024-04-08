import {
	Ed25519Keypair,
	Ed25519PrivateKey,
	Ed25519PublicKey
} from "@peerbit/crypto";

export const signKey = new Ed25519Keypair({
	publicKey: new Ed25519PublicKey({
		publicKey: new Uint8Array([
			5, 149, 176, 13, 66, 29, 75, 143, 214, 180, 65, 225, 86, 4, 119, 164, 133,
			242, 216, 14, 93, 209, 61, 169, 189, 187, 11, 119, 123, 38, 85, 62
		])
	}),
	privateKey: new Ed25519PrivateKey({
		privateKey: new Uint8Array([
			33, 26, 237, 82, 39, 39, 253, 88, 140, 102, 107, 38, 88, 61, 94, 198, 153,
			191, 15, 237, 202, 199, 19, 143, 26, 80, 99, 66, 102, 99, 63, 205
		])
	})
});
export const signKey2 = new Ed25519Keypair({
	publicKey: new Ed25519PublicKey({
		publicKey: new Uint8Array([
			0, 83, 117, 223, 84, 41, 239, 99, 197, 171, 102, 198, 110, 4, 225, 6, 135,
			52, 107, 232, 107, 134, 115, 112, 98, 202, 24, 88, 110, 2, 122, 236
		])
	}),
	privateKey: new Ed25519PrivateKey({
		privateKey: new Uint8Array([
			158, 191, 201, 210, 111, 174, 133, 245, 76, 53, 91, 75, 19, 154, 85, 113,
			119, 56, 13, 46, 211, 62, 233, 195, 142, 131, 12, 75, 176, 41, 177, 222
		])
	})
});
export const signKey3 = new Ed25519Keypair({
	publicKey: new Ed25519PublicKey({
		publicKey: new Uint8Array([
			38, 88, 36, 255, 43, 10, 168, 50, 178, 240, 103, 216, 196, 143, 196, 17,
			254, 112, 106, 68, 144, 157, 34, 9, 233, 209, 102, 16, 192, 20, 66, 139
		])
	}),
	privateKey: new Ed25519PrivateKey({
		privateKey: new Uint8Array([
			42, 241, 139, 40, 85, 71, 39, 66, 187, 79, 12, 209, 106, 137, 118, 102,
			142, 115, 6, 206, 129, 169, 246, 211, 52, 250, 216, 90, 66, 224, 36, 17
		])
	})
});
export const signKey4 = new Ed25519Keypair({
	publicKey: new Ed25519PublicKey({
		publicKey: new Uint8Array([
			63, 172, 10, 150, 69, 69, 219, 130, 58, 206, 216, 7, 168, 198, 223, 231,
			252, 190, 113, 252, 30, 77, 217, 189, 30, 187, 117, 70, 117, 111, 58, 93
		])
	}),
	privateKey: new Ed25519PrivateKey({
		privateKey: new Uint8Array([
			17, 27, 214, 212, 184, 229, 64, 94, 237, 212, 171, 81, 160, 136, 119, 141,
			85, 15, 45, 210, 157, 198, 193, 181, 220, 193, 164, 209, 51, 145, 96, 70
		])
	})
});
