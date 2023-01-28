import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Entry } from "../entry.js";
import { Ed25519Keypair, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

describe("Signed Log", function () {
	let signKey: KeyWithMeta<Ed25519Keypair>,
		signKey2: KeyWithMeta<Ed25519Keypair>;

	let keystore: Keystore;
	let store: BlockStore;

	beforeAll(async () => {
		rmrf.sync(testKeyStorePath(__filenameBase));

		await fs.copy(
			signingKeysFixturesPath(__dirname),
			testKeyStorePath(__filenameBase)
		);

		keystore = new Keystore(
			await createStore(testKeyStorePath(__filenameBase))
		);

		signKey = (await keystore.getKey(
			new Uint8Array([0])
		)) as KeyWithMeta<Ed25519Keypair>;
		signKey2 = (await keystore.getKey(
			new Uint8Array([1])
		)) as KeyWithMeta<Ed25519Keypair>;

		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();

		rmrf.sync(testKeyStorePath(__filenameBase));
		await keystore?.close();
	});

	it("has the correct identity", () => {
		const log = new Log(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		expect(log._identity.publicKey).toMatchSnapshot();
	});

	it("has the correct public key", () => {
		const log = new Log(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		expect(log._identity.publicKey).toEqual(signKey.keypair.publicKey);
	});

	it("has the correct pkSignature", () => {
		const log = new Log(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		expect(log._identity.publicKey).toEqual(signKey.keypair.publicKey);
	});

	it("has the correct signature", () => {
		const log = new Log(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		expect(log._identity.publicKey).toEqual(signKey.keypair.publicKey);
	});

	it("entries contain an identity", async () => {
		const log = new Log(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		await log.append("one");
		assert.notStrictEqual(await log.values[0].signatures, null);
		assert.deepStrictEqual(
			await log.values[0].signatures[0].publicKey,
			signKey.keypair.publicKey
		);
	});

	it("can sign with multiple identities", async () => {
		const log = new Log(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		const signers = [
			async (data: Uint8Array) =>
				new SignatureWithKey({
					publicKey: signKey.keypair.publicKey,
					signature: await signKey.keypair.sign(data),
				}),
			async (data: Uint8Array) =>
				new SignatureWithKey({
					publicKey: signKey2.keypair.publicKey,
					signature: await signKey2.keypair.sign(data),
				}),
		];

		await log.append("one", { signers });
		expect(
			await Promise.all(
				log.values[0].signatures.map((x) => x.publicKey.hashcode())
			)
		).toContainAllValues([
			await signKey.keypair.publicKey.hashcode(),
			await signKey2.keypair.publicKey.hashcode(),
		]);
	});

	// This test is not expected anymore (TODO what is the expected behaviour, enforce arbitrary conditions or put responibility on user)
	/* it('doesn\'t join logs with different IDs ', async () => {
  const log1 = new Log<string>(store, {
	...signKey.keypair,
	sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
  }, { logId: 'A' })
  const log2 = new Log<string>(store, {
	...signKey2.keypair,
	sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
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

  expect(err).toEqual(undefined)
  expect(log1._id).toEqual('A')
  expect(log1.values.length).toEqual(1)
  expect(log1.values[0].payload.getValue()).toEqual('one')
})
*/

	// We dont check signatues during join anymore
	it("throws an error if log is signed but the signature doesn't verify", async () => {
		const log1 = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		const log2 = new Log<string>(
			store,
			{
				...signKey2.keypair,
				sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let err;

		try {
			await log1.append("one");
			await log2.append("two");
			let entry: Entry<string> = log2.values[0];
			entry._signatures = await log1.values[0]._signatures;
			await log1.join(log2, { verifySignatures: true });
		} catch (e: any) {
			err = e.toString();
		}

		const entry = log2.values[0];
		expect(err).toEqual(
			`Error: Invalid signature entry with hash "${entry.hash}"`
		);
		expect(log1.values.length).toEqual(1);
		expect(log1.values[0].payload.getValue()).toEqual("one");
	});

	/* 
it('throws an error if entry doesn\'t have append access', async () => {
  const log1 = new Log<string>(store, {
	...signKey.keypair,
	sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
  }, { logId: 'A' })
  const log2 = new Log<string>(store, {
	...signKey2.keypair,
	sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
  }, { logId: 'A' })

  let err
  try {
	await log1.append('one')
	await log2.append('two')
	await log1.join(log2)
  } catch (e: any) {
	err = e.toString()
  }

  expect(err).toEqual(`Error: Could not append entry, key "${signKey2.keypair.publicKey}" is not allowed to write to the log`)
})

it('throws an error upon join if entry doesn\'t have append access', async () => {
	const canAppend: CanAppend<any> = async (_entry: any, signature: MaybeEncrypted<SignatureWithKey>) => signature.decrypted.getValue(SignatureWithKey).publicKey.equals(signKey.keypair.publicKey);
	const log1 = new Log<string>(store, {
	  ...signKey.keypair,
	  sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
	}, { logId: 'A' })
	const log2 = new Log<string>(store, {
	  ...signKey2.keypair,
	  sign: async (data: Uint8Array) => (await signKey2.keypair.sign(data))
	}, { logId: 'A' })

	let err
	try {
	  await log1.append('one')
	  await log2.append('two')
	  await log1.join(log2)
	} catch (e: any) {
	  err = e.toString()
	}

	expect(err).toEqual(`Error: Could not append Entry<T>, key "${signKey2.keypair.publicKey}" is not allowed to write to the log`)
  }) */
});
