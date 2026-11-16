import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { claimEntryOwnerNamespace } from "../src/entry-owner-namespace.js";
import { EventStore } from "./utils/stores/index.js";

describe("append delivery options — entry owner namespace reservation", () => {
	const id = () => new Uint8Array(32).fill(7);

	it("rejects a legacy alias while a protected projection owns the namespace", () => {
		const node = {};
		const log = {};
		const projection = claimEntryOwnerNamespace(node, log, id(), true, {});
		expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).to.throw(
			"incompatible owner",
		);
		expect(() => claimEntryOwnerNamespace(node, log, id(), false)).to.throw(
			"incompatible owner",
		);
		projection.releaseAfterTerminalCleanup();
		claimEntryOwnerNamespace(
			node,
			{},
			id(),
			false,
		).releaseAfterTerminalCleanup();
	});

	it("rejects protection until every legacy member has completed cleanup", () => {
		const node = {};
		const log = {};
		const first = claimEntryOwnerNamespace(node, log, id(), false, {});
		const runtime = claimEntryOwnerNamespace(node, log, id(), false);
		const second = claimEntryOwnerNamespace(node, {}, id(), false, {});
		first.releaseAfterTerminalCleanup();
		second.releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, log, id(), true, {})).to.throw(
			"incompatible owner",
		);
		runtime.releaseAfterTerminalCleanup();
		claimEntryOwnerNamespace(
			node,
			log,
			id(),
			true,
			{},
		).releaseAfterTerminalCleanup();
	});

	it("couples one protected projection to its exact SharedLog runtime", () => {
		const node = {};
		const log = {};
		const projection = claimEntryOwnerNamespace(node, log, id(), true, {});
		const runtime = claimEntryOwnerNamespace(node, log, id(), true);
		expect(() => claimEntryOwnerNamespace(node, {}, id(), true)).to.throw(
			"incompatible owner",
		);
		runtime.releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).to.throw(
			"incompatible owner",
		);
		projection.releaseAfterTerminalCleanup();
		claimEntryOwnerNamespace(
			node,
			{},
			id(),
			false,
		).releaseAfterTerminalCleanup();
	});

	it("allows the associated projection after the protected runtime claims first", () => {
		const node = {};
		const log = {};
		const runtime = claimEntryOwnerNamespace(node, log, id(), true);
		const projection = claimEntryOwnerNamespace(node, log, id(), true, {});
		projection.releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).to.throw(
			"incompatible owner",
		);
		runtime.releaseAfterTerminalCleanup();
	});

	it("rejects a second projection and re-entry even with the same SharedLog", () => {
		const node = {};
		const log = {};
		const owner = {};
		const projection = claimEntryOwnerNamespace(node, log, id(), true, owner);
		for (const nextOwner of [owner, {}]) {
			expect(() =>
				claimEntryOwnerNamespace(node, log, id(), true, nextOwner),
			).to.throw("protected projection");
		}
		projection.releaseAfterTerminalCleanup();
	});

	it("does not let a repeated stale release remove a replacement owner", () => {
		const node = {};
		const log = {};
		const first = claimEntryOwnerNamespace(node, log, id(), true, {});
		first.releaseAfterTerminalCleanup();
		const replacement = claimEntryOwnerNamespace(node, log, id(), true, {});
		first.releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).to.throw(
			"incompatible owner",
		);
		replacement.releaseAfterTerminalCleanup();
	});

	it("releases only its own token while another runtime reference remains", () => {
		const node = {};
		const log = {};
		const first = claimEntryOwnerNamespace(node, log, id(), true);
		const second = claimEntryOwnerNamespace(node, log, id(), true);
		first.releaseAfterTerminalCleanup();
		first.releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).to.throw(
			"incompatible owner",
		);
		second.releaseAfterTerminalCleanup();
	});

	it("keeps unrelated nodes and lower namespaces independent", () => {
		const node = {};
		const claims = [
			claimEntryOwnerNamespace(node, {}, id(), true, {}),
			claimEntryOwnerNamespace({}, {}, id(), true, {}),
			claimEntryOwnerNamespace(node, {}, new Uint8Array(32).fill(8), true, {}),
		];
		for (const claim of claims) claim.releaseAfterTerminalCleanup();
	});

	it("snapshots the namespace before caller-owned ID bytes change", () => {
		const node = {};
		const original = id();
		const claim = claimEntryOwnerNamespace(node, {}, original, true, {});
		original.fill(8);
		expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).to.throw(
			"incompatible owner",
		);
		const other = claimEntryOwnerNamespace(node, {}, original, true, {});
		claim.releaseAfterTerminalCleanup();
		claimEntryOwnerNamespace(
			node,
			{},
			id(),
			false,
		).releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, {}, original, false)).to.throw(
			"incompatible owner",
		);
		other.releaseAfterTerminalCleanup();
	});

	it("shares one reservation across separately evaluated module copies", async () => {
		const otherCopy: typeof import("../src/entry-owner-namespace.js") =
			await import(
				new URL(
					"../src/entry-owner-namespace.js?independent-namespace-copy",
					import.meta.url,
				).href
			);
		expect(otherCopy.claimEntryOwnerNamespace).not.equal(
			claimEntryOwnerNamespace,
		);
		const node = {};
		const log = {};
		const projection = claimEntryOwnerNamespace(node, log, id(), true, {});
		const runtime = otherCopy.claimEntryOwnerNamespace(node, log, id(), true);
		expect(() =>
			otherCopy.claimEntryOwnerNamespace(node, {}, id(), false),
		).throw("incompatible owner");
		expect(() =>
			otherCopy.claimEntryOwnerNamespace(node, log, id(), true, {}),
		).throw("protected projection");
		projection.releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).throw(
			"incompatible owner",
		);
		runtime.releaseAfterTerminalCleanup();
		const replacement = otherCopy.claimEntryOwnerNamespace(
			node,
			{},
			id(),
			false,
		);
		projection.releaseAfterTerminalCleanup();
		runtime.releaseAfterTerminalCleanup();
		expect(() => claimEntryOwnerNamespace(node, log, id(), true)).throw(
			"incompatible owner",
		);
		replacement.releaseAfterTerminalCleanup();
	});

	it("uses a non-enumerable node reservation and removes all terminal references", () => {
		const node = {};
		const key = Symbol.for("@peerbit/shared-log/entry-owner-namespaces");
		const claim = claimEntryOwnerNamespace(node, {}, id(), true, {});
		const descriptor = Object.getOwnPropertyDescriptor(node, key)!;
		expect(descriptor.enumerable).equal(false);
		const heldNamespaces = descriptor.value.namespaces as Map<string, unknown>;
		expect(heldNamespaces.size).equal(1);
		claim.releaseAfterTerminalCleanup();
		expect(heldNamespaces.size).equal(0);
		expect(Object.getOwnPropertyDescriptor(node, key)).undefined;
	});

	it("rejects unknown registry versions and malformed shapes without replacing them", () => {
		const key = Symbol.for("@peerbit/shared-log/entry-owner-namespaces");
		for (const value of [
			null,
			undefined,
			{ version: 2, namespaces: new Map() },
			{ version: 1, namespaces: {} },
		]) {
			const node = {};
			Object.defineProperty(node, key, { value });
			expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).throw(
				"version or shape",
			);
			expect(Object.getOwnPropertyDescriptor(node, key)!.value).equal(value);
		}
		for (const members of [
			null,
			{},
			new Map([[{}, { protectedStore: false }]]),
		]) {
			const node = {};
			const namespaces = new Map([["07".repeat(32), members]]);
			Object.defineProperty(node, key, { value: { version: 1, namespaces } });
			expect(() => claimEntryOwnerNamespace(node, {}, id(), false)).throw(
				"member shape",
			);
			expect(namespaces.get("07".repeat(32))).equal(members);
		}
		const node = {};
		const getter = sinon.spy(() => ({ version: 1, namespaces: new Map() }));
		Object.defineProperty(node, key, { get: getter });
		expect(() => claimEntryOwnerNamespace(node, {}, id(), true)).throw(
			"version or shape",
		);
		expect(getter.called).equal(false);
	});

	for (const terminal of ["close", "drop"] as const) {
		it(`retains the runtime reservation after failed ${terminal} until exact cleanup succeeds`, async () => {
			const session = await TestSession.disconnected(1);
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false },
			});
			const shared = store.log as any;
			const original = shared._close.bind(shared);
			const failure = new Error("injected namespace cleanup failure");
			const cleanup = sinon.stub(shared, "_close");
			cleanup.onFirstCall().rejects(failure);
			cleanup.callsFake(original);
			try {
				await expect(store[terminal]()).rejectedWith(failure);
				expect(() =>
					claimEntryOwnerNamespace(
						session.peers[0],
						{},
						shared.log.id,
						true,
						{},
					),
				).to.throw("incompatible owner");
				await store[terminal]();
				claimEntryOwnerNamespace(
					session.peers[0],
					{},
					shared.log.id,
					true,
					{},
				).releaseAfterTerminalCleanup();
				expect(cleanup.callCount).equal(2);
			} finally {
				cleanup.restore();
				await session.stop();
			}
		});
	}
});
