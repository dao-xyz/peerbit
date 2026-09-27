import { deserialize } from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { Documents, type Operation } from "../src/index.js";
import { Document, TestStore } from "./data.js";

describe("document join preflight", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.disconnected(2);
	});

	afterEach(async () => {
		sinon.restore();
		await session.stop();
	});

	it("rejects a head before fetching its missing document history", async () => {
		const store = new TestStore({ docs: new Documents<Document>() });
		const source = await session.peers[0].open(store.clone(), {
			args: { replicate: false, mode: "compat" },
		});
		await source.docs.put(new Document({ id: "key", name: "before" }));
		const { entry } = await source.docs.put(
			new Document({ id: "key", name: "after" }),
		);
		const inspected: string[] = [];
		const target = await session.peers[1].open(store.clone(), {
			args: {
				replicate: false,
				mode: "compat",
				log: {
					canJoin: (candidate) => {
						inspected.push(candidate.hash);
						return false;
					},
				},
			},
		});
		const requested: string[] = [];
		sinon.stub(target.docs.log.log.blocks, "get").callsFake(async (hash) => {
			requested.push(hash);
			throw new Error("Unexpected history resolution");
		});
		await target.docs.log.join([entry]);
		expect(inspected).to.deep.equal([entry.hash]);
		expect(requested).to.deep.equal([]);
		expect(target.docs.log.log.length).to.equal(0);
		expect(await target.docs.index.get("key", { remote: false })).to.be
			.undefined;
	});

	it("keeps canPerform mandatory after an accepted preflight", async () => {
		const store = new TestStore({ docs: new Documents<Document>() });
		const source = await session.peers[0].open(store.clone(), {
			args: { replicate: false, mode: "compat" },
		});
		const { entry } = await source.docs.put(new Document({ id: "key" }));
		const calls: string[] = [];
		const target = await session.peers[1].open(store.clone(), {
			args: {
				replicate: false,
				mode: "compat",
				log: {
					canJoin: () => {
						calls.push("preflight");
						return true;
					},
				},
				canPerform: () => {
					calls.push("authorization");
					return false;
				},
			},
		});
		await target.docs.log.join([entry]);
		expect(calls).to.deep.equal(["preflight", "authorization"]);
		expect(target.docs.log.log.length).to.equal(0);
	});

	it("does not let a preflight callback mutate the signed entry", async () => {
		const store = new TestStore({ docs: new Documents<Document>() });
		const source = await session.peers[0].open(store.clone(), {
			args: { replicate: false, mode: "compat" },
		});
		const { entry } = await source.docs.put(new Document({ id: "key" }));
		const target = await session.peers[1].open(store.clone(), {
			args: {
				replicate: false,
				mode: "compat",
				log: {
					canJoin: (candidate) => {
						candidate.meta.next.push("untrusted-parent");
						return true;
					},
				},
			},
		});
		await target.docs.log.join([entry]);
		expect(entry.meta.next).to.deep.equal([]);
		expect(target.docs.log.log.length).to.equal(1);
		expect(
			(await target.docs.index.get("key", { remote: false }))?.id,
		).to.equal("key");
	});

	it("cannot bypass remote signature admission by changing callback-local trust flags", async () => {
		const store = new TestStore({ docs: new Documents<Document>() });
		const source = await session.peers[0].open(store.clone(), {
			args: { replicate: false, mode: "compat" },
		});
		const { entry } = await source.docs.put(new Document({ id: "key" }));
		const bytes = await source.docs.log.log.blocks.get(entry.hash, {
			remote: false,
		});
		expect(bytes).to.exist;
		const remote = deserialize(new Uint8Array(bytes!), Entry) as Entry<Operation>;
		remote.init(source.docs.log.log);
		remote.createdLocally = false;
		remote.signatures[0]!.signature[0] ^= 1;
		remote.hash = await Entry.prepareMultihash(remote);
		expect(await remote.verifySignatures()).to.equal(false);
		let inspected = false;
		const target = await session.peers[1].open(store.clone(), {
			args: {
				replicate: false,
				mode: "compat",
				log: {
					canJoin: (candidate) => {
						inspected = true;
						candidate.createdLocally = true;
						candidate.verifySignatures = () => true;
						return true;
					},
				},
			},
		});
		await target.docs.log.join([remote]);
		expect(inspected).to.equal(true);
		expect(remote.createdLocally).to.equal(false);
		expect(await remote.verifySignatures()).to.equal(false);
		expect(target.docs.log.log.length).to.equal(0);
		expect(await target.docs.index.get("key", { remote: false })).to.be
			.undefined;
	});
});
