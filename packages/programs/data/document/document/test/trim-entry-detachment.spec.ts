import { ShallowEntry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

describe("document trim callback entry detachment", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.disconnected(1);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("isolates shallow metadata without changing trim callback semantics", async () => {
		let seed = "stable";
		const canTrimReceivers: unknown[] = [];
		const cacheIdReceivers: unknown[] = [];
		const callbackEntries: ShallowEntry[] = [];
		let expectedData: Uint8Array | undefined;
		let targetHash: string | undefined;
		const filter = {
			canTrim: function (this: unknown, entry: ShallowEntry) {
				canTrimReceivers.push(this);
				callbackEntries.push(entry);
				expect(entry).to.be.instanceOf(ShallowEntry);
				if (entry.hash === targetHash) {
					expect(entry.meta.data).to.deep.equal(expectedData);
					entry.meta.data![0] = 101;
					entry.meta.clock.id[0] = 102;
					entry.meta.next.push("callback-only");
					entry.meta.clock.timestamp.logical += 1000;
				}
				return false;
			},
			cacheId: function (this: unknown) {
				cacheIdReceivers.push(this);
				return seed;
			},
		};

		const store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					replicate: false,
					log: {
						trim: {
							type: "length",
							from: 2,
							to: 1,
							filter,
						},
					},
				},
			},
		);
		const first = await store.docs.put(
			new Document({ id: "first", name: "first" }),
			{
				meta: { data: new Uint8Array([9, 8]) },
				replicate: false,
				target: "none",
			},
		);
		const canonicalBefore = await store.docs.log.log.getShallow(
			first.entry.hash,
		);
		expect(canonicalBefore).to.exist;
		targetHash = first.entry.hash;
		expectedData = new Uint8Array(canonicalBefore!.meta.data!);
		const clockId = new Uint8Array(canonicalBefore!.meta.clock.id);
		const next = [...canonicalBefore!.meta.next];
		const logical = canonicalBefore!.meta.clock.timestamp.logical;

		await store.docs.put(new Document({ id: "second", name: "second" }), {
			replicate: false,
			target: "none",
		});

		const callbackFirst = callbackEntries.find(
			(entry) => entry.hash === first.entry.hash,
		);
		expect(callbackFirst).to.exist;
		expect(callbackFirst).not.to.equal(canonicalBefore);
		expect(callbackFirst!.meta).not.to.equal(canonicalBefore!.meta);
		expect(canTrimReceivers).to.have.length(callbackEntries.length);
		expect(
			canTrimReceivers.every((receiver) => receiver === undefined),
		).to.equal(true);
		expect(cacheIdReceivers).to.have.length(1);
		expect(cacheIdReceivers[0]).not.to.equal(filter);

		const canonicalAfter = await store.docs.log.log.getShallow(
			first.entry.hash,
		);
		expect(canonicalAfter).to.exist;
		expect(canonicalAfter!.meta.data).to.deep.equal(expectedData);
		expect(canonicalAfter!.meta.clock.id).to.deep.equal(clockId);
		expect(canonicalAfter!.meta.next).to.deep.equal(next);
		expect(canonicalAfter!.meta.clock.timestamp.logical).to.equal(logical);

		const callsAfterAppend = callbackEntries.length;
		await store.docs.log.log.trim();
		expect(callbackEntries).to.have.length(callsAfterAppend);
		seed = "changed";
		await store.docs.log.log.trim();
		expect(callbackEntries.length).to.be.greaterThan(callsAfterAppend);
		expect(canTrimReceivers).to.have.length(callbackEntries.length);
		expect(
			canTrimReceivers.every((receiver) => receiver === undefined),
		).to.equal(true);
		expect(cacheIdReceivers).to.have.length(3);
		expect(
			cacheIdReceivers.every((receiver) => receiver === cacheIdReceivers[0]),
		).to.equal(true);

		const canonicalAfterCacheReset = await store.docs.log.log.getShallow(
			first.entry.hash,
		);
		expect(canonicalAfterCacheReset!.meta.data).to.deep.equal(expectedData);
		expect(canonicalAfterCacheReset!.meta.clock.id).to.deep.equal(clockId);
		expect(canonicalAfterCacheReset!.meta.next).to.deep.equal(next);
		expect(canonicalAfterCacheReset!.meta.clock.timestamp.logical).to.equal(
			logical,
		);
	});
});
