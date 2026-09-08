import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	Context,
	ResultIndexedValue,
	Results,
} from "@peerbit/document-interface";
import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { Documents, type Operation, type PutOperation } from "../src/index.js";
import { Document, TestStore } from "./data.js";

type AdmissionSeam = {
	_canAppend(
		entry: Entry<Operation>,
		reference: { document: Document; operation: PutOperation },
	): Promise<Operation | false>;
	canAppend(entry: Entry<Operation>): Promise<boolean>;
	_canAppendDecodedDocuments: WeakMap<PutOperation, Document>;
};
type ContextKind = "same" | "older" | "equal" | "newer";

describe("immutable admission — synthetic context controls", () => {
	let session: TestSession;
	let target: TestStore;
	let independent: { entry: Entry<Operation>; document: Document };
	let dependent: typeof independent;
	let authorize = true;
	let authorizationCalls: string[] = [];
	const sandbox = sinon.createSandbox();
	const seam = () => target.docs as unknown as AdmissionSeam;

	before(async () => {
		session = await TestSession.disconnected(2);
		const args = {
			mode: "compat" as const,
			replicate: false as const,
			nativeGraph: false as const,
			nativeBackbone: false as const,
			nativeRangePlanner: false as const,
		};
		const source = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>({ immutable: true }) }),
			{ args },
		);
		target = await session.peers[1].open(source.clone(), {
			args: {
				...args,
				canPerform: (properties) => {
					if (properties.type === "put") {
						authorizationCalls.push(properties.value.id);
					}
					return authorize;
				},
			},
		});
		const document = new Document({ id: "independent", name: "original" });
		independent = {
			document,
			entry: (await source.docs.put(document, { unique: true, target: "none" }))
				.entry,
		};
		const withDependency = new Document({ id: "dependent", name: "original" });
		dependent = {
			document: withDependency,
			entry: (
				await source.docs.put(withDependency, {
					unique: true,
					target: "none",
					meta: { next: [independent.entry] },
				})
			).entry,
		};
		expect(independent.entry.meta.next).to.have.length(0);
		expect(dependent.entry.meta.next).to.deep.equal([independent.entry.hash]);
	});
	beforeEach(() => {
		authorize = true;
		authorizationCalls = [];
	});
	afterEach(() => sandbox.restore());
	after(async () => session?.stop());

	// Only the context sets are fabricated. These tests pin admission predicates,
	// not authenticated RPC response ordering, remote coverage or log commitment.
	const stubContexts = (
		groups: ContextKind[][] | undefined,
		entry: Entry<Operation>,
		document: Document,
	) => {
		const responses = groups?.map(
			(group) =>
				new Results({
					kept: 0n,
					results: group.map((kind) => {
						const created =
							entry.meta.clock.timestamp.wallTime +
							(kind === "equal" ? 0n : kind === "newer" ? 1n : -1n);
						return new ResultIndexedValue({
							source: serialize(document),
							indexed: document,
							entries: [],
							context: new Context({
								created,
								modified: created,
								head: kind === "same" ? entry.hash : `synthetic-${kind}`,
								gid: "synthetic-context",
								size: 0,
							}),
						});
					}),
				}),
		);
		return sandbox
			.stub(target.docs.index, "getDetailed")
			.resolves(responses as any);
	};
	const assertLookup = (
		lookup: ReturnType<typeof stubContexts>,
		id: string,
	) => {
		expect(lookup.callCount).to.equal(1);
		expect(lookup.firstCall.args[0]).to.have.property("primitive", id);
		expect(lookup.firstCall.args[1]).to.deep.equal({
			resolve: false,
			local: true,
			remote: { strategy: "fallback" },
		});
	};
	const cases: Array<[string, ContextKind[][] | undefined, boolean, boolean]> =
		[
			["empty before older", [[], ["older"]], false, false],
			["older before empty", [["older"], []], false, false],
			["newer then older within a set", [["newer", "older"]], false, false],
			["older then newer within a set", [["older", "newer"]], false, false],
			["newer then older across sets", [["newer"], ["older"]], false, false],
			["older then newer across sets", [["older"], ["newer"]], false, false],
			[
				"same head cannot mask another older head",
				[["same"], ["older"]],
				false,
				false,
			],
			["same head remains idempotent", [["same"]], false, true],
			["same head retains dependency pointers", [["same"]], true, true],
			[
				"equal timestamps retain the strict comparison",
				[["equal"]],
				false,
				true,
			],
			["only newer heads", [["newer"], ["newer"]], false, true],
			["equal head forbids dependency pointers", [["equal"]], true, false],
			["newer head forbids dependency pointers", [["newer"]], true, false],
			[
				"same head cannot mask another head with next",
				[["same"], ["newer"]],
				true,
				false,
			],
			["undefined lookup retains dependency pointers", undefined, true, true],
			["empty lookup retains dependency pointers", [[], []], true, true],
		];
	for (const [name, groups, hasNext, allowed] of cases) {
		it(name, async () => {
			const { entry, document } = hasNext ? dependent : independent;
			const operation = (await entry.getPayloadValue()) as PutOperation;
			const lookup = stubContexts(groups, entry, document);
			const result = await seam()._canAppend(entry, { document, operation });
			expect(result).to.equal(allowed ? operation : false);
			assertLookup(lookup, document.id);
			expect(authorizationCalls).to.deep.equal([]);
			expect(seam()._canAppendDecodedDocuments.has(operation)).to.equal(false);
		});
	}

	for (const outcome of ["allow", "deny", "conflict"] as const) {
		it(`handles received-entry ${outcome} with one decode and no retained callback cache`, async () => {
			// Deserialize genuine signed source bytes, but invoke admission directly:
			// this is a decode/authorization control, not wire or signature validation.
			const { document } = independent;
			const received = (
				deserialize(serialize(independent.entry), Entry) as Entry<Operation>
			).init(independent.entry);
			const originalBytes = serialize(received);
			const operation = (await received.getPayloadValue()) as PutOperation;
			const lookup = stubContexts(
				[["same"], [outcome === "conflict" ? "older" : "equal"]],
				received,
				document,
			);
			const decode = sandbox.spy(target.docs.index.valueEncoding, "decoder");
			authorize = outcome !== "deny";
			expect(await seam().canAppend(received)).to.equal(outcome === "allow");
			assertLookup(lookup, document.id);
			expect(decode.callCount).to.equal(1);
			expect(authorizationCalls).to.deep.equal(
				outcome === "conflict" ? [] : [document.id],
			);
			expect(seam()._canAppendDecodedDocuments.has(operation)).to.equal(false);
			expect(serialize(received)).to.deep.equal(originalBytes);
		});
	}
});
