import { deserialize, serialize } from "@dao-xyz/borsh";
import type { Change, Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import {
	ExchangeHeadsMessage,
	createExchangeHeadsMessages,
} from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import type { SyncProfileEvent } from "../src/sync/index.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore, type Operation } from "./utils/stores/event-store.js";

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-batch-canappend-signatures",
};

type Fixture = {
	session: TestSession;
	source: EventStore<string, any>;
	target: EventStore<string, any>;
	message: ExchangeHeadsMessage<any>;
	entries: Entry<Operation<string>>[];
	profile: SyncProfileEvent[];
};

const createFixture = async (
	count: number,
	canAppend: (entry: Entry<Operation<string>>) => boolean | Promise<boolean>,
	onChange?: (change: Change<Operation<string>>) => void,
): Promise<Fixture> => {
	const session = await TestSession.disconnected(2);
	const store = new EventStore<string, any>();
	const profile: SyncProfileEvent[] = [];
	const source = await session.peers[0].open(store.clone(), {
		args: {
			replicate: false,
			setup,
			timeUntilRoleMaturity: 0,
		},
	});
	const target = await session.peers[1].open(store.clone(), {
		args: {
			replicate: { factor: 1 },
			setup,
			canAppend,
			onChange,
			sync: { profile: (event) => profile.push(event) },
			timeUntilRoleMaturity: 0,
		},
	});

	const hashes: string[] = [];
	for (let i = 0; i < count; i++) {
		const { entry } = await source.add(`value-${i}-${uuid()}`, {
			meta: { next: [] },
		});
		hashes.push(entry.hash);
	}
	const messages: ExchangeHeadsMessage<any>[] = [];
	for await (const generated of createExchangeHeadsMessages(
		source.log.log,
		hashes,
	)) {
		messages.push(
			deserialize(
				serialize(generated),
				TransportMessage,
			) as ExchangeHeadsMessage<any>,
		);
	}
	expect(messages).to.have.length(1);
	const message = messages[0]!;
	expect(message).to.be.instanceOf(ExchangeHeadsMessage);
	return {
		session,
		source,
		target,
		message,
		entries: message.heads.map((head) => head.entry),
		profile,
	};
};

const deliver = (fixture: Fixture) =>
	fixture.target.log.onMessage(fixture.message, {
		from: fixture.source.node.identity.publicKey,
	} as any);

const batchProfileEvents = (fixture: Fixture) =>
	fixture.profile.filter(
		(event) =>
			event.name === "sharedLog.canAppendBatch.verifySignatures" &&
			event.details?.programCanAppendDeferred === true,
	);

describe("receive admission batch signatures with canAppend", function () {
	this.timeout(120_000);

	it("copies every native verifier input before the receive await", async () => {
		const fixture = await createFixture(1, () => true);
		const sharedLog = fixture.target.log as any;
		const aliases = {
			signature: new Uint8Array([1, 2, 3]),
			publicKey: new Uint8Array([4, 5, 6]),
			message: new Uint8Array([7, 8, 9]),
		};
		const prepare = sinon
			.stub(sharedLog, "prepareNativeEd25519VerificationBatch")
			.returns([aliases]);
		try {
			const [input] = sharedLog.prepareReceiveNativeEd25519VerificationBatch(
				fixture.entries,
			);
			for (const field of ["signature", "publicKey", "message"] as const) {
				expect(input[field]).to.deep.equal(aliases[field]);
				expect(input[field]).to.not.equal(aliases[field]);
				aliases[field][0]! ^= 0xff;
				expect(input[field][0]).to.not.equal(aliases[field][0]);
			}
		} finally {
			prepare.restore();
			await fixture.session.stop();
		}
	});

	it("uses the native verifier at 16 entries without changing callback arguments", async () => {
		for (const count of [15, 16]) {
			const callbackEntries: Entry<Operation<string>>[] = [];
			const fixture = await createFixture(count, (entry) => {
				callbackEntries.push(entry);
				return true;
			});
			const verifySpies = fixture.entries.map((entry) =>
				sinon.spy(entry, "verifySignatures"),
			);
			try {
				await deliver(fixture);

				expect(callbackEntries).to.deep.equal(fixture.entries);
				expect(fixture.target.log.log.length).to.equal(count);
				expect(batchProfileEvents(fixture)).to.have.length(
					count === 16 ? 1 : 0,
				);
				expect(verifySpies.map((spy) => spy.callCount)).to.deep.equal(
					new Array(count).fill(count === 16 ? 0 : 1),
				);
			} finally {
				for (const spy of verifySpies) spy.restore();
				await fixture.session.stop();
			}
		}
	});

	it("runs mixed allow and deny callbacks once in input order", async () => {
		const callbackEntries: Entry<Operation<string>>[] = [];
		const changes: Change<Operation<string>>[] = [];
		let denied = new Set<string>();
		const fixture = await createFixture(
			16,
			(entry) => {
				callbackEntries.push(entry);
				return !denied.has(entry.hash);
			},
			(change) => changes.push(change),
		);
		denied = new Set([fixture.entries[3]!.hash, fixture.entries[9]!.hash]);
		try {
			await deliver(fixture);

			expect(callbackEntries).to.deep.equal(fixture.entries);
			expect(fixture.target.log.log.length).to.equal(14);
			for (const entry of fixture.entries) {
				expect(await fixture.target.log.log.has(entry.hash)).to.equal(
					!denied.has(entry.hash),
				);
			}
			const addedHashes = changes.flatMap((change) =>
				change.added.map(({ entry }) => entry.hash),
			);
			expect(addedHashes).to.deep.equal(
				fixture.entries
					.filter((entry) => !denied.has(entry.hash))
					.map((entry) => entry.hash),
			);
			expect(batchProfileEvents(fixture)).to.have.length(1);
		} finally {
			await fixture.session.stop();
		}
	});

	it("rejects an invalid signature before the callback and does not leak it into retry", async () => {
		const callbackEntries: Entry<Operation<string>>[] = [];
		const fixture = await createFixture(16, (entry) => {
			callbackEntries.push(entry);
			return true;
		});
		const invalidEntry = fixture.entries[7]!;
		const signature = invalidEntry.signatures[0]!.signature;
		const originalByte = signature[0]!;
		signature[0] = originalByte ^ 0xff;
		const verifySpy = sinon.spy(invalidEntry, "verifySignatures");
		try {
			await deliver(fixture);

			expect(callbackEntries).to.deep.equal(
				fixture.entries.filter((entry) => entry !== invalidEntry),
			);
			expect(await fixture.target.log.log.has(invalidEntry.hash)).to.equal(
				false,
			);
			expect(fixture.target.log.log.length).to.equal(15);
			expect(verifySpy.callCount).to.equal(0);
			expect(
				(fixture.target.log as any)._receiveSignatureVerificationFacts.has(
					invalidEntry,
				),
			).to.equal(false);

			// The second receive contains one missing entry, so it deliberately takes
			// the scalar verifier. A stale false lease would reject this valid retry.
			signature[0] = originalByte;
			callbackEntries.length = 0;
			await deliver(fixture);
			expect(callbackEntries).to.deep.equal([invalidEntry]);
			expect(await fixture.target.log.log.has(invalidEntry.hash)).to.equal(
				true,
			);
			expect(fixture.target.log.log.length).to.equal(16);
			expect(verifySpy.callCount).to.equal(1);
		} finally {
			verifySpy.restore();
			signature[0] = originalByte;
			await fixture.session.stop();
		}
	});

	it("rechecks a later entry mutated by an earlier callback", async () => {
		const callbackEntries: Entry<Operation<string>>[] = [];
		let fixtureEntries: Entry<Operation<string>>[] = [];
		let mutated = false;
		const fixture = await createFixture(16, (entry) => {
			callbackEntries.push(entry);
			if (!mutated) {
				mutated = true;
				fixtureEntries[8]!.signatures[0]!.signature[0]! ^= 0xff;
			}
			return true;
		});
		fixtureEntries = fixture.entries;
		const mutatedEntry = fixture.entries[8]!;
		const originalByte = mutatedEntry.signatures[0]!.signature[0]!;
		const verifySpies = fixture.entries.map((entry) =>
			sinon.spy(entry, "verifySignatures"),
		);
		try {
			await deliver(fixture);

			expect(callbackEntries).to.deep.equal(
				fixture.entries.filter((entry) => entry !== mutatedEntry),
			);
			expect(await fixture.target.log.log.has(mutatedEntry.hash)).to.equal(
				false,
			);
			expect(fixture.target.log.log.length).to.equal(15);
			expect(verifySpies[8]!.callCount).to.equal(1);
			expect(
				verifySpies
					.filter((_, index) => index !== 8)
					.every((spy) => spy.callCount === 0),
			).to.equal(true);
		} finally {
			for (const spy of verifySpies) spy.restore();
			mutatedEntry.signatures[0]!.signature[0] = originalByte;
			await fixture.session.stop();
		}
	});

	it("clears leases after an asynchronous callback throws", async () => {
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const callbackEntries: Entry<Operation<string>>[] = [];
		let shouldThrow = true;
		let failingHash: string | undefined;
		const fixture = await createFixture(16, async (entry) => {
			callbackEntries.push(entry);
			if (shouldThrow && entry.hash === failingHash) {
				entered.resolve();
				await release.promise;
				throw new Error("intentional canAppend failure");
			}
			return true;
		});
		failingHash = fixture.entries[4]!.hash;
		try {
			const receive = deliver(fixture);
			await entered.promise;
			expect(callbackEntries).to.deep.equal(fixture.entries.slice(0, 5));
			// Ordinary joins preserve their committed prefix while an asynchronous
			// callback is pending or throws on the following entry.
			expect(fixture.target.log.log.length).to.equal(4);
			release.resolve();
			let receiveRejected = false;
			try {
				await receive;
			} catch {
				receiveRejected = true;
			}
			// onMessage classifies a program callback error as a handled receive
			// failure; it logs and resolves while preserving the committed prefix.
			expect(receiveRejected).to.equal(false);
			expect(fixture.target.log.log.length).to.equal(4);
			for (const entry of fixture.entries) {
				expect(
					(fixture.target.log as any)._receiveSignatureVerificationFacts.has(
						entry,
					),
				).to.equal(false);
			}

			shouldThrow = false;
			callbackEntries.length = 0;
			await deliver(fixture);
			expect(callbackEntries).to.deep.equal(fixture.entries.slice(4));
			expect(fixture.target.log.log.length).to.equal(16);
		} finally {
			release.resolve();
			await fixture.session.stop();
		}
	});

	it("keeps a concurrent same-entry lease after the other receive finishes", async () => {
		const fixture = await createFixture(1, () => true);
		const sharedLog = fixture.target.log as any;
		const entry = fixture.entries[0]!;
		const storageBytes = entry.getStorageBytes().slice();
		const firstEntered = pDefer<void>();
		const firstRelease = pDefer<void>();
		const secondEntered = pDefer<void>();
		const secondRelease = pDefer<void>();
		const verifySpy = sinon.spy(entry, "verifySignatures");
		const fact = () => ({
			entry,
			storageBytes: storageBytes.slice(),
			verified: true,
		});
		try {
			const first = sharedLog.withReceiveSignatureVerificationFacts(
				[fact()],
				async () => {
					firstEntered.resolve();
					await firstRelease.promise;
				},
			);
			await firstEntered.promise;

			const second = sharedLog.withReceiveSignatureVerificationFacts(
				[fact()],
				async () => {
					secondEntered.resolve();
					await secondRelease.promise;
				},
			);
			await secondEntered.promise;
			expect(
				sharedLog._receiveSignatureVerificationFacts.get(entry),
			).to.have.length(2);

			firstRelease.resolve();
			await first;
			expect(
				sharedLog._receiveSignatureVerificationFacts.get(entry),
			).to.have.length(1);
			expect(await sharedLog.canAppend(entry)).to.equal(true);
			expect(verifySpy.callCount).to.equal(0);

			secondRelease.resolve();
			await second;
			expect(sharedLog._receiveSignatureVerificationFacts.has(entry)).to.equal(
				false,
			);
			expect(await sharedLog.canAppend(entry)).to.equal(true);
			expect(verifySpy.callCount).to.equal(1);
		} finally {
			firstRelease.resolve();
			secondRelease.resolve();
			verifySpy.restore();
			await fixture.session.stop();
		}
	});

	it("drains an admitted receive and clears leases when close cancels its lifecycle", async () => {
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const callbackEntries: Entry<Operation<string>>[] = [];
		let first = true;
		const fixture = await createFixture(16, async (entry) => {
			callbackEntries.push(entry);
			if (first) {
				first = false;
				entered.resolve();
				await release.promise;
			}
			return true;
		});
		try {
			const receive = deliver(fixture);
			await entered.promise;
			for (const entry of fixture.entries) {
				expect(
					(fixture.target.log as any)._receiveSignatureVerificationFacts.has(
						entry,
					),
				).to.equal(true);
			}

			let closeSettled = false;
			const close = fixture.target.close().then(() => {
				closeSettled = true;
			});
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			expect(
				(fixture.target.log as any)._instanceLifecycle
					.membershipLifecycleController.signal.aborted,
			).to.equal(true);
			expect(closeSettled).to.equal(false);

			release.resolve();
			await Promise.all([receive, close]);
			expect(callbackEntries).to.deep.equal(fixture.entries);
			for (const entry of fixture.entries) {
				expect(
					(fixture.target.log as any)._receiveSignatureVerificationFacts.has(
						entry,
					),
				).to.equal(false);
			}
		} finally {
			release.resolve();
			await fixture.session.stop();
		}
	});
});
