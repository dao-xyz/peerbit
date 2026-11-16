import {
	ClosedError,
	type Program,
	TerminalOperationNotStartedError,
} from "@peerbit/program";
import { SharedLog } from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import type { Operation } from "../src/operation.js";
import { Documents } from "../src/program.js";
import { DocumentIndex, type OpenOptions } from "../src/search.js";
import { Document } from "./data.js";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};
type Index = DocumentIndex<Document, Document, any>;
type InternalIndex = {
	openWithDeferredQuery: (
		properties: OpenOptions<Document, Document, any>,
		assertOwnerActive?: () => void,
	) => Promise<() => Promise<void>>;
};

describe("document index deferred query opening", () => {
	let session: TestSession;
	let index: Index;
	let properties: OpenOptions<Document, Document, any>;
	let prepare: () => Promise<() => Promise<void>>;

	beforeEach(async () => {
		session = await TestSession.connected(1);
		index = new DocumentIndex();
		await index.beforeOpen(session.peers[0]);
		const log = new SharedLog<Operation>();
		sinon.stub(log, "isReplicating").resolves(true);
		properties = {
			log,
			documentType: Document,
			dbType: Documents,
			documentEvents: new EventTarget() as any,
			replicate: async () => {},
			maybeOpen: async (value) => value,
			prefetch: true,
		};
		prepare = () =>
			(index as unknown as InternalIndex).openWithDeferredQuery(properties);
	});

	afterEach(async () => {
		sinon.restore();
		await index?.close();
		await session?.stop();
	});

	it("keeps ordinary open's query and predictor activation", async () => {
		const open = sinon.spy(index._query, "open");
		const add = sinon.spy(index._query.events, "addEventListener");
		await index.open(properties);
		expect(open.calledOnce).to.be.true;
		expect(
			add.getCalls().filter((call) => call.args[0] === "join"),
		).to.have.length(1);
		expect(await index.getSize()).to.equal(0);
	});

	it("prepares local state and changes without query or prefetch networking until completion", async () => {
		const open = sinon.spy(index._query, "open");
		const add = sinon.spy(index._query.events, "addEventListener");
		const changes = sinon.spy(properties.documentEvents, "addEventListener");
		const finish = await prepare();
		expect(index.index).to.exist; // Deliberately available for private recovery.
		expect(
			changes.getCalls().filter((call) => call.args[0] === "change"),
		).to.have.length(1);
		expect(open.called).to.be.false;
		expect(
			add.getCalls().filter((call) => call.args[0] === "join"),
		).to.have.length(0);
		expect(() => index.getSize()).to.throw(ClosedError);
		expect(() => index.iterate({}, { remote: false })).to.throw(ClosedError);
		await expect(
			index.get("missing", { remote: false, waitFor: 10 }),
		).to.be.rejectedWith(ClosedError);
		await expect(
			index.getDetailed("missing", { remote: false }),
		).to.be.rejectedWith(ClosedError);
		await expect(index.search({}, { remote: false })).to.be.rejectedWith(
			ClosedError,
		);
		await expect(index.afterOpen()).to.be.rejectedWith(ClosedError);
		await expect(index.waitFor([])).to.be.rejectedWith(ClosedError);
		const first = finish();
		expect(finish()).to.equal(first);
		await first;
		await finish();
		expect(open.calledOnce).to.be.true;
		expect(
			add.getCalls().filter((call) => call.args[0] === "join"),
		).to.have.length(1);
		expect(await index.getSize()).to.equal(0);
	});

	for (const terminal of ["close", "drop"] as const) {
		it(`rejects stale completion after ${terminal} and permits a new same-instance generation`, async () => {
			const open = sinon.spy(index._query, "open");
			const old = await prepare();
			await index[terminal]();
			await expect(old()).to.be.rejectedWith(ClosedError);
			expect(open.called).to.be.false;
			await index.beforeOpen(session.peers[0]);
			const current = await prepare();
			await expect(old()).to.be.rejectedWith(ClosedError);
			expect(open.called).to.be.false;
			await current();
			expect(open.calledOnce).to.be.true;
		});
	}

	it("does not activate after local preparation fails", async () => {
		const open = sinon.spy(index._query, "open");
		properties.documentType = class Unsupported {} as any;
		await expect(prepare()).to.be.rejectedWith("missing @variant");
		expect(open.called).to.be.false;
		expect(() => index.getSize()).to.throw(ClosedError);
		await expect(prepare()).to.be.rejectedWith("terminal cleanup");
	});

	it("keeps local and remote queries closed when the recovered owner is poisoned", async () => {
		let failure: Error | undefined;
		const finish = await (
			index as unknown as InternalIndex
		).openWithDeferredQuery(properties, () => {
			if (failure) throw failure;
		});
		await finish();
		expect(await index.getSize()).equal(0);
		failure = new Error("owner needs recovery");
		expect(() => index.getSize()).throw(failure.message);
		expect(() => index.iterate({}, { remote: false })).throw(failure.message);
		await expect(index.get("missing", { remote: false })).rejectedWith(
			failure.message,
		);
		await expect(index.search({}, { remote: false })).rejectedWith(
			failure.message,
		);
		const process = sinon.spy(index, "processQuery");
		await (index as any).queryRPCResponseHandler(
			{},
			{ from: session.peers[0].identity.publicKey },
		);
		expect(process.called).equal(false);
		await expect(
			index.processQuery({} as any, session.peers[0].identity.publicKey, false),
		).rejectedWith(failure.message);
	});

	it("does not expose query RPC if the owner fails before activation", async () => {
		const open = sinon.spy(index._query, "open");
		const finish = await (
			index as unknown as InternalIndex
		).openWithDeferredQuery(properties, () => {
			throw new Error("owner not recovered");
		});
		await expect(finish()).rejectedWith("owner not recovered");
		expect(open.called).equal(false);
		expect(() => index.getSize()).throw(ClosedError);
	});

	it("withholds an in-flight RPC response if its owner becomes inactive", async () => {
		let active = true;
		const open = sinon.spy(index._query, "open");
		const finish = await (
			index as unknown as InternalIndex
		).openWithDeferredQuery(properties, () => {
			if (!active) throw new Error("owner poisoned");
		});
		await finish();
		const started = deferred();
		const held = deferred();
		sinon.stub(index as any, "queryRPCResponseHandler").callsFake(async () => {
			started.resolve();
			await held.promise;
			return "stale response";
		});
		const response = open.firstCall.args[0]!.responseHandler!(
			{} as any,
			{} as any,
		);
		await started.promise;
		active = false;
		held.resolve();
		expect(await response).undefined;
	});

	it("does not retry a failed activation or leave its predictor attached", async () => {
		const open = sinon
			.stub(index._query, "open")
			.rejects(new Error("query failed"));
		const remove = sinon.spy(index._query.events, "removeEventListener");
		const finish = await prepare();
		await expect(finish()).to.be.rejectedWith("query failed");
		await expect(finish()).to.be.rejectedWith(ClosedError);
		expect(open.calledOnce).to.be.true;
		expect(
			remove.getCalls().filter((call) => call.args[0] === "join"),
		).to.have.length(1);
		expect(() => index.getSize()).to.throw(ClosedError);
	});

	it("rejects terminal admission during unabortable activation and permits cleanup after settlement", async () => {
		const started = deferred();
		const held = deferred();
		const order: string[] = [];
		const originalOpen = index._query.open.bind(index._query);
		sinon.stub(index._query, "open").callsFake(async (args) => {
			started.resolve();
			await held.promise;
			await originalOpen(args);
			order.push("opened");
		});
		const originalClose = index._query.close.bind(index._query);
		sinon.stub(index._query, "close").callsFake(async (from) => {
			order.push("closing");
			return originalClose(from);
		});
		const finish = await prepare();
		const activation = finish();
		await started.promise;
		await expect(index.close()).to.be.rejectedWith(
			TerminalOperationNotStartedError,
		);
		await expect(index.drop()).to.be.rejectedWith(
			TerminalOperationNotStartedError,
		);
		expect(order).to.deep.equal([]);
		held.resolve();
		await activation;
		expect(await index.getSize()).to.equal(0);
		await index.close();
		expect(order).to.deep.equal(["opened", "closing"]);
	});

	it("owns a queued activation before its first turn", async () => {
		const open = sinon.spy(index._query, "open");
		const finish = await prepare();
		const activation = finish();
		await expect(index.close()).to.be.rejectedWith(
			TerminalOperationNotStartedError,
		);
		await activation;
		expect(open.calledOnce).to.be.true;
		await index.close();
	});

	it("rejects terminal admission during local preparation without opening queries", async () => {
		const entered = deferred();
		const held = deferred();
		const indexer = session.peers[0].indexer;
		const scope = indexer.scope.bind(indexer);
		sinon.stub(indexer, "scope").callsFake(async (...args) => {
			entered.resolve();
			await held.promise;
			return scope(...args);
		});
		const open = sinon.spy(index._query, "open");
		const preparing = prepare();
		await entered.promise;
		await expect(index.close()).to.be.rejectedWith(
			TerminalOperationNotStartedError,
		);
		await expect(index.drop()).to.be.rejectedWith(
			TerminalOperationNotStartedError,
		);
		held.resolve();
		const finish = await preparing;
		await index.close();
		await expect(finish()).to.be.rejectedWith(ClosedError);
		expect(open.called).to.be.false;
	});

	for (const terminal of ["close", "drop"] as const) {
		it(`preserves a surviving parent after nonterminal ${terminal} and rejects an unrelated owner`, async () => {
			const first: Program<any> = new Documents();
			const second: Program<any> = new Documents();
			index.parents = [first, second];
			first.children = [index];
			second.children = [index];
			const finish = await prepare();
			expect(await index[terminal](first)).to.be.false;
			await finish();
			await expect(index[terminal](new Documents())).to.be.rejectedWith(
				"Could not find from in parents",
			);
			expect(await index.getSize()).to.equal(0);
			await finish();
			expect(await index[terminal](second)).to.be.true;
		});
	}

	it("refuses private preparation while ordinary open is physically pending", async () => {
		const entered = deferred();
		const held = deferred();
		const original = index._query.open.bind(index._query);
		sinon.stub(index._query, "open").callsFake(async (args) => {
			entered.resolve();
			await held.promise;
			return original(args);
		});
		const opening = index.open(properties);
		await entered.promise;
		await expect(prepare()).to.be.rejectedWith("index is opening");
		held.resolve();
		await opening;
		expect(await index.getSize()).to.equal(0);
	});
});
