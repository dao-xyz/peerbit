import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { expect } from "chai";
import type { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";
import { signKey, signKey2 } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("signature", function () {
	let store: BlockStore;

	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	it("has the correct identity", async () => {
		const log = new Log();
		await log.open(store, signKey);
		expect(log.identity.publicKey).to.equal(signKey.publicKey);
	});

	it("has the correct public key", async () => {
		const log = new Log();
		await log.open(store, signKey);
		expect(log.identity.publicKey).equal(signKey.publicKey);
	});

	it("has the correct pkSignature", async () => {
		const log = new Log();
		await log.open(store, signKey);
		expect(log.identity.publicKey).equal(signKey.publicKey);
	});

	it("has the correct signature", async () => {
		const log = new Log();
		await log.open(store, signKey);
		expect(log.identity.publicKey).equal(signKey.publicKey);
	});

	it("entries contain an identity", async () => {
		const log = new Log();
		await log.open(store, signKey, { encoding: JSON_ENCODING });
		await log.append("one");
		expect((await log.toArray())[0].signatures).to.exist;
		expect(
			(await log.toArray())[0].signatures[0].publicKey.equals(
				signKey.publicKey,
			),
		).to.be.true;
	});

	it("can sign with multiple identities", async () => {
		const log = new Log();
		await log.open(store, signKey, { encoding: JSON_ENCODING });
		const signers = [signKey.sign.bind(signKey), signKey2.sign.bind(signKey2)];

		await log.append("one", { signers });
		expect(
			await Promise.all(
				(await log.toArray())[0].signatures.map((x) => x.publicKey.hashcode()),
			),
		).to.have.members([
			await signKey.publicKey.hashcode(),
			await signKey2.publicKey.hashcode(),
		]);
	});

	// This test is not expected anymore (TODO what is the expected behaviour, enforce arbitrary conditions or put responibility on user)
	/* it('doesn\'t join logs with different IDs ', async () => {
  const log1 = new Log<Uint8Array>(store, {
	...signKey,
	sign: async (data: Uint8Array) => (await signKey.sign(data))
  }, { logId: 'A' })
  const log2 = new Log<Uint8Array>(store, {
	...signKey2,
	sign: async (data: Uint8Array) => (await signKey2.sign(data))
  }, { logId: 'B' })

  let err
  try {
	await log1.append('one')
	await log2.append('two')
	await log2.append('three')
	await log1.join(log2)
  } catch (e: any) {
	err = e.toString()
	throw e
  }

  expect(err).equal(undefined)
  expect(log1._id).equal('A')
  expect(log1.length).equal(1)
  expect(log1.values[0].payload.getValue()).equal('one')
})
*/

	// We dont check signatues during join anymore
	it("throws an error if log is signed but the signature doesn't verify", async () => {
		const log1 = new Log<Uint8Array>();
		await log1.open(store, signKey);
		const log2 = new Log<Uint8Array>();
		await log2.open(store, signKey2);
		let err;

		try {
			await log1.append(new Uint8Array([1]));
			await log2.append(new Uint8Array([2]));
			let entry: Entry<Uint8Array> = (await log2.toArray())[0];
			entry._signatures = (await log1.toArray())[0]._signatures;
			await log1.join(log2, { verifySignatures: true });
		} catch (e: any) {
			err = e.toString();
		}

		const entry = (await log2.toArray())[0];
		expect(err).equal(
			`Error: Invalid signature entry with hash "${entry.hash}"`,
		);
		expect((await log1.toArray()).length).equal(1);
		expect((await log1.toArray())[0].payload.getValue()).to.deep.equal(
			new Uint8Array([1]),
		);
	});

	/* 
it('throws an error if entry doesn\'t have append access', async () => {
  const log1 = new Log<Uint8Array>(store, {
	...signKey,
	sign: async (data: Uint8Array) => (await signKey.sign(data))
  }, { logId: 'A' })
  const log2 = new Log<Uint8Array>(store, {
	...signKey2,
	sign: async (data: Uint8Array) => (await signKey2.sign(data))
  }, { logId: 'A' })

  let err
  try {
	await log1.append('one')
	await log2.append('two')
	await log1.join(log2)
  } catch (e: any) {
	err = e.toString()
  }

  expect(err).equal(`Error: Could not append entry, key "${signKey2.publicKey}" is not allowed to write to the log`)
})

it('throws an error upon join if entry doesn\'t have append access', async () => {
	const canAppend: CanAppend<any> = async (_entry: any, signature: MaybeEncrypted<SignatureWithKey>) => signature.decrypted.getValue(SignatureWithKey).publicKey.equals(signKey.publicKey);
	const log1 = new Log<Uint8Array>(store, {
	  ...signKey,
	  sign: async (data: Uint8Array) => (await signKey.sign(data))
	}, { logId: 'A' })
	const log2 = new Log<Uint8Array>(store, {
	  ...signKey2,
	  sign: async (data: Uint8Array) => (await signKey2.sign(data))
	}, { logId: 'A' })

	let err
	try {
	  await log1.append('one')
	  await log2.append('two')
	  await log1.join(log2)
	} catch (e: any) {
	  err = e.toString()
	}

	expect(err).equal(`Error: Could not append Entry<T>, key "${signKey2.publicKey}" is not allowed to write to the log`)
  }) */
});
