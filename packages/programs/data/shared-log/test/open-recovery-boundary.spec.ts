import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { SharedLog } from "../src/index.js";

type Recovery = (signal: AbortSignal) => Promise<void>;
@variant("test_shared_log_owner_recovery")
class RecoveryStore extends Program<{
	recover?: Recovery;
	nativeGraph?: boolean;
}> {
	@field({ type: SharedLog })
	log = new SharedLog<Uint8Array>();

	async open(options?: { recover?: Recovery; nativeGraph?: boolean }) {
		const args = {
			replicate: false as const,
			nativeGraph: options?.nativeGraph ?? false,
			nativeBackbone: false as const,
			nativeRangePlanner: false as const,
		};
		if (options?.recover) {
			await (
				this.log as unknown as {
					openWithRecovery(
						options: typeof args,
						recover: Recovery,
					): Promise<void>;
				}
			).openWithRecovery(args, options.recover);
		} else {
			await this.log.open(args);
		}
	}
}

describe("replicate owner recovery before shared-log communication", () => {
	let session: TestSession;
	beforeEach(async () => {
		session = await TestSession.disconnected(1);
	});
	afterEach(async () => {
		await session.stop();
	});

	for (const nativeGraph of [false, true]) {
		it(`opens lower storage before recovery and communications only afterwards (nativeGraph=${nativeGraph})`, async () => {
			const store = new RecoveryStore();
			const entered = pDefer();
			const release = pDefer();
			const rpcOpen = sinon.spy(store.log.rpc, "open");
			const open = session.peers[0].open(store, {
				args: {
					nativeGraph,
					recover: async (signal) => {
						expect(signal.aborted).equal(false);
						expect(store.log.log.closed).equal(false);
						expect(
							Boolean(store.log.log.entryIndex.properties.nativeGraph),
						).equal(nativeGraph);
						expect(store.log.entryCoordinatesIndex).not.equal(undefined);
						entered.resolve();
						await release.promise;
					},
				},
			});
			try {
				await entered.promise;
				expect(rpcOpen.callCount).equal(0);
				expect(store.log.syncronizer).equal(undefined);
				await expect(store.log.append(Uint8Array.of(9))).rejectedWith(
					"owner recovery has not completed",
				);
				await expect(store.log.open({ replicate: false })).rejectedWith(
					"reopen through its recovery owner",
				);
				await expect(store.open({ recover: async () => {} })).rejectedWith(
					"requires a closed lower log",
				);
			} finally {
				release.resolve();
			}
			await open;
			expect(rpcOpen.callCount).equal(1);
			expect(store.log.syncronizer).not.equal(undefined);
		});
	}

	it("refuses appends after lower storage opens but before native intent and owner recovery finish", async () => {
		const store = new RecoveryStore();
		const entered = pDefer();
		const release = pDefer();
		const internal = store.log as unknown as {
			recoverNativeStrictDurableTransactionIntent(
				documentIndexReady?: boolean,
			): Promise<boolean>;
		};
		const recoverIntent =
			internal.recoverNativeStrictDurableTransactionIntent.bind(internal);
		const heldIntent = sinon
			.stub(internal, "recoverNativeStrictDurableTransactionIntent")
			.callsFake(async (documentIndexReady) => {
				expect(store.log.log.closed).equal(false);
				entered.resolve();
				await release.promise;
				return recoverIntent(documentIndexReady);
			});
		const ownerRecovery = sinon.spy(async () => {});
		const rpcOpen = sinon.spy(store.log.rpc, "open");
		const lowerAppend = sinon.spy(store.log.log, "append");
		const opening = session.peers[0].open(store, {
			args: { recover: ownerRecovery },
		});
		try {
			await entered.promise;
			expect(ownerRecovery.called).equal(false);
			expect(rpcOpen.called).equal(false);
			await expect(store.log.append(Uint8Array.of(1))).rejectedWith(
				"owner recovery has not completed",
			);
			expect(lowerAppend.called).equal(false);
			expect(store.log.log.length).equal(0);
		} finally {
			release.resolve();
		}
		await opening;
		expect(ownerRecovery.calledOnce).equal(true);
		expect(rpcOpen.calledOnce).equal(true);
		heldIntent.restore();
		await store.log.append(Uint8Array.of(2));
		expect(store.log.log.length).equal(1);
		await store.close();
	});

	it("does not expose a failed recovery and can reopen after terminal cleanup", async () => {
		const store = new RecoveryStore();
		const rpcOpen = sinon.spy(store.log.rpc, "open");
		const failure = new Error("retained marker recovery failed");
		await expect(
			session.peers[0].open(store, {
				args: {
					recover: async () => {
						throw failure;
					},
				},
			}),
		).rejectedWith(failure.message);
		expect(rpcOpen.callCount).equal(0);
		expect(store.log.log.closed).equal(true);
		let recovered = 0;
		await session.peers[0].open(store, {
			args: {
				recover: async () => {
					recovered++;
				},
			},
		});
		expect(recovered).equal(1);
		expect(rpcOpen.callCount).equal(1);
	});

	it("poisons a direct failed recovery until cleanup, without a plain-open fallback", async () => {
		const store = new RecoveryStore();
		await store.beforeOpen(session.peers[0]);
		try {
			await expect(
				store.open({
					recover: async () => {
						throw new Error("recovery stopped");
					},
				}),
			).rejectedWith("recovery stopped");
			await expect(store.log.append(Uint8Array.of(1))).rejectedWith(
				"ownership",
			);
			await expect(store.log.log.append(Uint8Array.of(1))).rejectedWith(
				"poisoned",
			);
			await expect(store.open({ recover: async () => {} })).rejectedWith(
				"requires a closed lower log",
			);
			await expect(store.log.open({ replicate: false })).rejectedWith(
				"reopen through its recovery owner",
			);
		} finally {
			await store.close();
		}
		await store.beforeOpen(session.peers[0]);
		await store.open({ recover: async () => {} });
		await store.log.append(Uint8Array.of(2));
		await store.close();
	});

	for (const stage of ["storage", "recovery", "rpc"] as const) {
		for (const terminal of ["close", "drop"] as const) {
			it(`rejects direct ${terminal} during protected ${stage} opening before teardown`, async () => {
				const store = new RecoveryStore();
				const entered = pDefer();
				const release = pDefer();
				const pause = async () => {
					entered.resolve();
					await release.promise;
				};
				const scope = session.peers[0].indexer.scope.bind(
					session.peers[0].indexer,
				);
				const scopeStub =
					stage === "storage"
						? sinon
								.stub(session.peers[0].indexer, "scope")
								.callsFake(async (...args) => {
									await pause();
									return scope(...args);
								})
						: undefined;
				const rpcOpen = store.log.rpc.open.bind(store.log.rpc);
				const rpcStub =
					stage === "rpc"
						? sinon.stub(store.log.rpc, "open").callsFake(async (...args) => {
								await pause();
								return rpcOpen(...args);
							})
						: undefined;
				const lowerTerminal = sinon.spy(store.log.log, terminal);
				// Exercise the direct child lifecycle. Handler-managed opens instead
				// reserve the graph and reject terminal calls until opening settles.
				await store.beforeOpen(session.peers[0]);
				const opening = store.open({
					recover: async (signal) => {
						if (stage === "recovery") await pause();
						expect(signal.aborted).equal(false);
					},
				});
				try {
					await entered.promise;
					await expect(store.log[terminal](store)).rejectedWith(
						"opening must settle",
					);
					expect(lowerTerminal.callCount).equal(0);
				} finally {
					release.resolve();
				}
				await opening;
				await store.log[terminal](store);
				expect(store.log.log.closed).equal(true);
				await store.close();
				scopeStub?.restore();
				rpcStub?.restore();
			});
		}
	}

	it("rejects protected opening while ordinary storage setup still owns the instance", async () => {
		const store = new RecoveryStore();
		await store.beforeOpen(session.peers[0]);
		const entered = pDefer();
		const release = pDefer();
		const original = session.peers[0].indexer.scope.bind(
			session.peers[0].indexer,
		);
		const scope = sinon
			.stub(session.peers[0].indexer, "scope")
			.callsFake(async (...args) => {
				entered.resolve();
				await release.promise;
				return original(...args);
			});
		const opening = store.open();
		try {
			await entered.promise;
			expect(store.log.log.closed).equal(true);
			await expect(store.open({ recover: async () => {} })).rejectedWith(
				"requires a closed lower log",
			);
		} finally {
			release.resolve();
		}
		await opening;
		scope.restore();
		await store.log.append(Uint8Array.of(1));
		await store.close();
	});

	it("normalizes empty recovery failures before poisoning lower mutations", async () => {
		const store = new RecoveryStore();
		await store.beforeOpen(session.peers[0]);
		try {
			await expect(
				store.open({ recover: async () => Promise.reject(undefined) }),
			).rejectedWith("owner recovery failed");
			await expect(store.log.log.append(Uint8Array.of(1))).rejectedWith(
				"poisoned",
			);
		} finally {
			await store.close();
		}
	});

	it("keeps Handler's open reservation while recovery is pending", async () => {
		const store = new RecoveryStore();
		const entered = pDefer();
		const release = pDefer();
		const opening = session.peers[0].open(store, {
			args: {
				recover: async () => {
					entered.resolve();
					await release.promise;
				},
			},
		});
		try {
			await entered.promise;
			await expect(store.log.close(store)).rejectedWith(
				"open generation owns its address",
			);
		} finally {
			release.resolve();
		}
		await opening;
		await store.close();
	});

	it("preserves ordinary public open without a recovery callback", async () => {
		const store = new RecoveryStore();
		const rpcOpen = sinon.spy(store.log.rpc, "open");
		await session.peers[0].open(store);
		expect(rpcOpen.callCount).equal(1);
		await store.log.append(Uint8Array.of(1));
		expect(store.log.log.length).equal(1);
	});
});
