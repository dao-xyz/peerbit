import { deserialize, serialize } from "@dao-xyz/borsh";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/index.js";

describe("observer", () => {
	let session: TestSession;
	const waitTimeout = 120 * 1000;
	const TRACE_OBSERVER_DIAG =
		process.env.PEERBIT_TRACE_OBSERVER_TESTS === "1" ||
		process.env.PEERBIT_TRACE_ALL_TEST_FAILURES === "1";

	const emitObserverDiag = async (
		label: string,
		stores: EventStore<string, any>[],
	) => {
		if (!TRACE_OBSERVER_DIAG) {
			return;
		}
		const rows = await Promise.all(
			stores.map(async (store) => {
				try {
					return {
						id: store.node.identity.publicKey.hashcode(),
						length: store.log.log.length,
						isReplicating: await store.log.isReplicating(),
						replicationIndexSize: (await store.log.replicationIndex?.getSize()) ?? "n/a",
						totalParticipation: await store.log.calculateTotalParticipation(),
						myParticipation: await store.log.calculateMyTotalParticipation(),
					};
				} catch (error: any) {
					return {
						id: store.node.identity.publicKey.hashcode(),
						error: error?.message ?? String(error),
					};
				}
			}),
		);
		console.error(`[observer-diag] ${label}: ${JSON.stringify(rows)}`);
	};

	before(async () => {});

	afterEach(async () => {
		await session.stop();
	});

	it("observers will not receive heads by default", async function () {
		this.timeout(waitTimeout + 30_000);
		session = await TestSession.disconnected(3);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);
		await session.peers[0].services.pubsub.waitFor(session.peers[1].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await session.peers[1].services.pubsub.waitFor(session.peers[0].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await session.peers[1].services.pubsub.waitFor(session.peers[2].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await session.peers[2].services.pubsub.waitFor(session.peers[1].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});

		let stores: EventStore<string, any>[] = [];
		const s = new EventStore<string, any>();
		const createStore = () => deserialize(serialize(s), EventStore);
		let replicatorEndIndex = 1;

		for (const [i, peer] of session.peers.entries()) {
			const store = await peer.open(createStore(), {
				args: {
					replicate: i <= replicatorEndIndex ? { factor: 1 } : false,
				},
			});
			stores.push(store);
		}

		await stores[0].waitFor(session.peers[1].peerId, {
			seek: "present",
			timeout: waitTimeout,
		});
		await stores[1].waitFor(session.peers[0].peerId, {
			seek: "present",
			timeout: waitTimeout,
		});
		await stores[1].waitFor(session.peers[2].peerId, {
			seek: "present",
			timeout: waitTimeout,
		});
		await stores[2].waitFor(session.peers[1].peerId, {
			seek: "present",
			timeout: waitTimeout,
		});

		await stores[0].log.waitForReplicator(session.peers[1].identity.publicKey, {
			timeout: waitTimeout,
		});
		await stores[1].log.waitForReplicator(session.peers[0].identity.publicKey, {
			timeout: waitTimeout,
		});

		const hashes: string[] = [];
		for (let i = 0; i < 100; i++) {
			hashes.push((await stores[0].add(String(i))).entry.hash);
		}

		try {
			await waitForResolved(
				() => expect(stores[1].log.log.length).to.equal(hashes.length),
				{ timeout: waitTimeout + 60_000, delayInterval: 1_000 },
			);
		} catch (error) {
			await emitObserverDiag(
				"observers will not receive heads by default::replicator lag",
				stores,
			);
			throw error;
		}

		const isReplicating = await Promise.all(
			stores.map((store) => store.log.isReplicating()),
		);
		try {
			for (let i = 0; i < hashes.length; i++) {
				for (let j = 1; j < stores.length; j++) {
					const hash = hashes[i];
					if (isReplicating[j]) {
						expect(await stores[j].log.log.has(hash)).to.be.true;
					} else {
						expect(await stores[j].log.log.has(hash)).to.be.false;
					}
				}
			}
		} catch (error) {
			await emitObserverDiag(
				"observers will not receive heads by default::hash-presence",
				stores,
			);
			throw error;
		}
	});

	it("target all will make heads reach observers", async () => {
		session = await TestSession.connected(2);
		await session.connect([[session.peers[0], session.peers[1]]]);

		const s = new EventStore<string, any>();
		const createStore = () => deserialize(serialize(s), EventStore);
		const fanout = {
			root: (session.peers[0].services as any).fanout.publicKeyHash as string,
			channel: {
				msgRate: 10,
				msgSize: 256,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: true,
			},
			join: { timeoutMs: 10_000 },
		};

		const replicator = await session.peers[0].open(createStore(), {
			args: {
				replicate: {
					factor: 1,
				},
				fanout,
			},
		});

		const observer = await session.peers[1].open(createStore(), {
			args: {
				replicate: false,
				keep: () => true,
				fanout,
			},
		});
		await waitForResolved(async () =>
			expect(await replicator.log.replicationIndex?.getSize()).equal(1),
		);
		await waitForResolved(async () =>
			expect(await observer.log.replicationIndex?.getSize()).equal(1),
		);

		await replicator.add("a", { target: "all" });
		/* await replicator.add("b", { target: 'replicators' }) */
		/* 
				await waitForResolved(() => expect(replicator.log.log.length).equal(2)); */
		await waitForResolved(() => expect(observer.log.log.length).equal(1));
	});

	it("can wait for replicator", async function () {
		this.timeout(waitTimeout + 30_000);
		session = await TestSession.disconnected(3);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);

		// Ensure pubsub neighbour streams are established in this sparse topology.
		// `waitForReplicator()` relies on RPC requests/responses being routed via pubsub DirectStream,
		// and without these streams the first few requests can be dropped and never reach the target.
		await session.peers[0].services.pubsub.waitFor(session.peers[1].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await session.peers[1].services.pubsub.waitFor(session.peers[0].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await session.peers[1].services.pubsub.waitFor(session.peers[2].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await session.peers[2].services.pubsub.waitFor(session.peers[1].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});

		const fanout0: any = (session.peers[0].services as any).fanout;
		const fanout1: any = (session.peers[1].services as any).fanout;
		const fanout2: any = (session.peers[2].services as any).fanout;
		await fanout0.waitFor(session.peers[1].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await fanout1.waitFor(session.peers[0].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await fanout1.waitFor(session.peers[2].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});
		await fanout2.waitFor(session.peers[1].peerId, {
			target: "neighbor",
			timeout: waitTimeout,
		});

		const s = new EventStore<string, any>();
		const createStore = () => deserialize(serialize(s), EventStore);
		const replicator = await session.peers[0].open(createStore(), {
			args: {
				replicate: { factor: 1 },
			},
		});

		const observer = await session.peers[2].open(createStore(), {
			args: {
				replicate: false,
			},
		});

		await observer.log.waitForReplicator(replicator.node.identity.publicKey, {
			timeout: waitTimeout,
		});
		expect(await observer.log.replicationIndex?.getSize()).equal(1);
		expect(await observer.log.isReplicating()).to.be.false;
	});
});
