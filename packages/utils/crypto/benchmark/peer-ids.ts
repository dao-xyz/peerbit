import { deserialize, serialize } from "@dao-xyz/borsh";
import { generateKeyPair } from "@libp2p/crypto/keys";
import { peerIdFromPublicKey } from "@libp2p/peer-id";
import B from "benchmark";
import { Ed25519Keypair, Ed25519PublicKey } from "../src/ed25519.js";

//node --loader ts-node/esm ./benchmark/peer-ids.ts

const keypair = await Ed25519Keypair.create();
const peerId = await generateKeyPair("Ed25519");
const peerIdPublicKey = peerIdFromPublicKey(peerId.publicKey);
const suite = new B.Suite("ed25519");

// TODO  this benchmark makes no sense to do anymore since libp2p 2.0.0. What we want to compare is the ser/der perform of libp2p peerid vs peerbit peerid
suite
	.add("PublicSignKey", {
		fn: async () => {
			deserialize(serialize(keypair.publicKey), Ed25519PublicKey);
		},
	})
	.add("PeerId ", {
		fn: () => {
			peerIdFromPublicKey(peerIdPublicKey.publicKey);
		},
	})
	.on("error", (error: any) => {
		throw error;
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.run();
