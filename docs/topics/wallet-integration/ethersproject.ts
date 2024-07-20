import { field, variant } from "@dao-xyz/borsh";
import { Wallet } from "@ethersproject/wallet";
import {
	type Identity,
	PreHash,
	Secp256k1PublicKey,
	SignatureWithKey,
} from "@peerbit/crypto";
import { Documents } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { v4 as uuid } from "uuid";

const wallet = Wallet.createRandom(); // if you would run this in the browser you would fetch the wallet from the window object instead

// We will force the wallet to sign a dummy message to recover the publickey
const walletPublicKey = await Secp256k1PublicKey.recover(wallet);

// From the wallet we can create a Peerbit compatible identity
const walletIdentity: Identity<Secp256k1PublicKey> = {
	publicKey: walletPublicKey,
	sign: async (bytes, prehashFn) => {
		if (prehashFn && prehashFn !== PreHash.ETH_KECCAK_256) {
			throw new Error("Expecting Ethereum wallet to use keccak256 hashing");
		}

		const signature = await wallet.signMessage(bytes);
		const signatureBytes = Buffer.from(signature);

		const signatureWithKey = new SignatureWithKey({
			prehash: PreHash.ETH_KECCAK_256,
			publicKey: walletPublicKey,
			signature: signatureBytes,
		});
		return signatureWithKey;
	},
};

@variant(0)
class Post {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	message: string;

	constructor(message: string) {
		this.id = uuid();
		this.message = message;
	}
}

@variant("post-store")
class PostStore extends Program {
	@field({ type: Documents })
	posts: Documents<Post>;

	constructor() {
		super();
		this.posts = new Documents();
	}

	async open(args?: any): Promise<void> {
		await this.posts.open({
			type: Post,
			canPerform: (properties) => {
				// This canPerfom will only return true if the post was signed by the wallet
				const publicKeys = properties.entry.publicKeys;
				if (publicKeys.find((publicKey) => publicKey.equals(walletPublicKey))) {
					return true;
				}

				return false;
			},
		});
	}
}

const peer = await Peerbit.create();
const db = await peer.open(new PostStore());

await db.posts.put(new Post("Hello world!"), {
	signers: [walletIdentity.sign.bind(walletIdentity)],
});

expect(await db.posts.index.getSize()).equal(1); // Post was appproved
await peer.stop();
