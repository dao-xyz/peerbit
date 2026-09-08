import { Ed25519Keypair } from "@peerbit/crypto";
import { Timestamp } from "@peerbit/log";
import { PersistedDeliveryError } from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	Documents,
	SearchRequestIndexed,
	StringMatch,
	toId,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

const persisted = (wallTime: bigint) => ({
	delivery: { reliability: "persisted" as const, minAcks: 1 },
	meta: { timestamp: new Timestamp({ wallTime }) },
});

describe("immutable document admission", function () {
	this.timeout(30_000);
	let session: TestSession;
	let sandbox: sinon.SinonSandbox;
	let stores: TestStore[];
	let releaseGates: (() => void)[];
	let pending: Promise<unknown>[];

	before(async () => {
		session = await TestSession.disconnected(3);
	});

	beforeEach(() => {
		sandbox = sinon.createSandbox();
		stores = [];
		releaseGates = [];
		pending = [];
	});

	afterEach(async () => {
		for (const release of releaseGates) release();
		try {
			await Promise.all(pending);
		} finally {
			try {
				sandbox.restore();
			} finally {
				await Promise.all(stores.map((store) => store.close()));
			}
		}
	});

	after(async () => {
		await session?.stop();
	});

	const open = async (peer: number) => {
		const store = new TestStore({
			docs: new Documents<Document>({ immutable: true }),
		});
		stores.push(store);
		await session.peers[peer].open(store, {
			args: {
				mode: "compat",
				replicate: false,
				keep: "self",
				nativeGraph: false,
				nativeBackbone: false,
				nativeRangePlanner: false,
			},
		});
		expect((store.docs as any).isNativeMode()).equal(false);
		return store.docs;
	};

	const expectQuery = (
		query: sinon.SinonStub,
		id: string,
		targets: string[],
	) => {
		expect(query.callCount).equal(1);
		const [request, options] = query.firstCall.args;
		expect(request).instanceOf(SearchRequestIndexed);
		expect(request.query).to.have.length(1);
		expect(request.query[0]).instanceOf(StringMatch);
		expect(request.query[0].value).equal(id);
		expect([...options.mode.to].sort()).deep.equal([...targets].sort());
	};

	for (const order of ["empty-first", "older-first"] as const) {
		it(`rejects a newer immutable value with complete ${order} remote responses`, async () => {
			const writer = await open(0);
			const empty = await open(1);
			const older = await open(2);
			const id = "remote-immutable-value";
			sandbox.stub(older.log, "getCover").resolves([]);
			await older.put(new Document({ id, name: "older" }), {
				target: "none",
				meta: { timestamp: new Timestamp({ wallTime: 1_000_000n }) },
			});
			const keys = session.peers
				.slice(1)
				.map((peer) => peer.identity.publicKey);
			const targets = keys.map((key) => key.hashcode());
			sandbox.stub(writer.log, "getCover").resolves(targets);
			let responses: Awaited<ReturnType<typeof empty.index.processQuery>>[] =
				[];
			const query = sandbox
				.stub((writer.index as any)._query, "request")
				.callsFake(async (rawRequest: unknown) => {
					const request = rawRequest as SearchRequestIndexed;
					responses = await Promise.all(
						[empty, older].map((docs) =>
							docs.index.processQuery(
								request,
								session.peers[0].identity.publicKey,
								false,
							),
						),
					);
					const complete = responses.map((response, i) => ({
						response,
						from: keys[i],
					}));
					return order === "empty-first" ? complete : complete.reverse();
				});
			// A rejection must never reach settlement; no receipt is fabricated.
			const settlement = sandbox
				.stub(writer.log as any, "settlePersistedDelivery")
				.rejects(new Error("unexpected settlement after immutable rejection"));
			const error = await writer
				.put(new Document({ id, name: "newer" }), persisted(2_000_000n))
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			// Inspect the genuine response data outside canAppend so a caught test
			// assertion/query error cannot masquerade as correct admission denial.
			expectQuery(query, id, targets);
			expect(responses).to.have.length(2);
			expect(responses[0].results).to.have.length(0);
			expect(responses[1].results).to.have.length(1);
			expect(responses[1].results[0].context.created).equal(1_000_000n);
			expect(error)
				.instanceOf(Error)
				.and.not.instanceOf(PersistedDeliveryError);
			expect((error as Error).message).match(/Not allowed to append/);
			expect(settlement.callCount).equal(0);
			expect(writer.log.log.length).equal(0);
			expect(await writer.index.index.get(toId(id))).equal(undefined);
		});
	}

	it("holds local commit before immutable lookup completes, then admits an empty partial result", async () => {
		const writer = await open(0);
		const empty = await open(1);
		const liveKey = session.peers[1].identity.publicKey;
		const missingHash = (await Ed25519Keypair.create()).publicKey.hashcode();
		const targets = [liveKey.hashcode(), missingHash];
		const cover = sandbox.stub(writer.log, "getCover").resolves(targets);
		const entered = pDefer<void>(),
			release = pDefer<void>();
		releaseGates.push(() => release.resolve());
		let response:
			| Awaited<ReturnType<typeof empty.index.processQuery>>
			| undefined;
		const query = sandbox
			.stub((writer.index as any)._query, "request")
			.callsFake(async (rawRequest: unknown) => {
				const request = rawRequest as SearchRequestIndexed;
				response = await empty.index.processQuery(
					request,
					session.peers[0].identity.publicKey,
					false,
				);
				entered.resolve();
				await release.promise;
				// Keep the real missing-response tolerance path, but do not wait for
				// a transport timeout or claim a durability receipt.
				return [{ response, from: liveKey }];
			});
		const sentinel = new Error("settlement reached; no durability receipt");
		let committedHash: string | undefined;
		const settlement = sandbox
			.stub(writer.log as any, "settlePersistedDelivery")
			.callsFake(async (rawRecords: unknown) => {
				const records = rawRecords as { canonicalHash: string }[];
				expect(records).to.have.length(1);
				committedHash = records[0].canonicalHash;
				expect(await writer.log.log.has(committedHash)).equal(true);
				throw sentinel;
			});
		const id = "gated-immutable-value";
		const operation = writer
			.put(new Document({ id, name: "new" }), persisted(3_000_000n))
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		pending.push(operation);
		await Promise.race([
			entered.promise,
			operation.then((error) => {
				throw error ?? new Error("put completed before the lookup gate");
			}),
		]);
		expectQuery(query, id, targets);
		expect(response?.results).to.have.length(0);
		expect(cover.firstCall.args[1]?.reachableOnly).equal(false);
		expect(writer.log.log.length).equal(0);
		expect(await writer.index.index.get(toId(id))).equal(undefined);
		expect(settlement.callCount).equal(0);
		release.resolve();
		const error = await operation;
		expect(error).instanceOf(PersistedDeliveryError);
		expect((error as PersistedDeliveryError).cause).equal(sentinel);
		expect((error as PersistedDeliveryError).localCommitSucceeded).equal(true);
		expect((error as PersistedDeliveryError).committedHashes).deep.equal([
			committedHash,
		]);
		expect(writer.log.log.length).equal(1);
		expect(settlement.callCount).equal(1);
		expect((await writer.index.index.get(toId(id)))?.value.name).equal("new");
	});

	it("rejects a newer local immutable value without a remote lookup", async () => {
		const writer = await open(0);
		const id = "local-immutable-value";
		const cover = sandbox.stub(writer.log, "getCover").resolves([]);
		await writer.put(new Document({ id, name: "older" }), {
			target: "none",
			meta: { timestamp: new Timestamp({ wallTime: 1_000_000n }) },
		});
		cover.resetHistory();
		const query = sandbox
			.stub((writer.index as any)._query, "request")
			.rejects(new Error("unexpected remote query for a local immutable hit"));
		const settlement = sandbox
			.stub(writer.log as any, "settlePersistedDelivery")
			.rejects(new Error("unexpected settlement after immutable rejection"));
		const error = await writer
			.put(new Document({ id, name: "newer" }), persisted(2_000_000n))
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(error).instanceOf(Error).and.not.instanceOf(PersistedDeliveryError);
		expect((error as Error).message).match(/Not allowed to append/);
		expect(query.callCount).equal(0);
		expect(cover.callCount).equal(0);
		expect(settlement.callCount).equal(0);
		expect(writer.log.log.length).equal(1);
		expect((await writer.index.index.get(toId(id)))?.value.name).equal("older");
	});
});
