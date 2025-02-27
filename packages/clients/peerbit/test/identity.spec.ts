import { Ed25519Keypair } from "@peerbit/crypto";
import { type ProgramClient } from "@peerbit/program";
import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

describe(`identity`, function () {
	describe("restore", () => {
		let client: ProgramClient;

		afterEach(async () => {
			await client?.stop();
		});

		it("disc", async () => {
			const directory = "./tmp/disc/" + +new Date();
			client = await Peerbit.create({ directory });
			const id1 = client.peerId;

			const kp = await Ed25519Keypair.create();
			await client.services.keychain?.import({
				keypair: kp,
				id: new Uint8Array([1, 2, 3]),
			});

			// stop
			await client.stop();

			// reopen same dir
			client = await Peerbit.create({ directory });

			expect(client.peerId.equals(id1)).to.be.true;

			const exportedKeypair = await client.services.keychain?.exportByKey(
				kp.publicKey,
			);
			expect(exportedKeypair!.equals(kp)).to.be.true;
		});

		/*  TODO this does not throw in the browser, but it should when we are building from multiple tabs? or not?
		it("accessing same directory throws", async () => {
			const directory = "./tmp/disc/" + +new Date();
			client = await Peerbit.create({ directory });
			let didNotThrow = false;
			try {
				const anotherClient = await Peerbit.create({ directory });
				const scope = await anotherClient.indexer.scope("test");
				await scope.start();
				const index = await scope.init({ schema: Schema });
				await index.put(new Schema("1"));
				didNotThrow = true;
			} catch (error: any) {
				// TODO assert error type
				console.log("Got expected error", error.message);
			}

			await delay(5000);
			expect(didNotThrow).to.be.false;
		}); */

		it("memory", async () => {
			client = await Peerbit.create({});
			const id1 = client.peerId;

			const kp = await Ed25519Keypair.create();
			await client.services.keychain?.import({
				keypair: kp,
				id: new Uint8Array([1, 2, 3]),
			});

			// stop
			await client.stop();

			// reopen, expect a clean slate
			client = await Peerbit.create({});
			expect(client.peerId.equals(id1)).to.be.false;
			expect(await client.services.keychain?.exportByKey(kp.publicKey)).equal(
				undefined,
			);
		});
	});
});
