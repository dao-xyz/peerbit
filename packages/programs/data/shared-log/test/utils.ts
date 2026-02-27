import { type Constructor } from "@dao-xyz/borsh";
import type { PublicSignKey } from "@peerbit/crypto";
import type { Entry } from "@peerbit/log";
import type { ProgramClient } from "@peerbit/program";
import type { TopicControlPlane } from "@peerbit/pubsub";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	type EntryWithRefs,
	ExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import {
	type ReplicationDomainHash,
	type SharedLog,
	createReplicationDomainHash,
	maxReplicas,
} from "../src/index.js";
import type { TransportMessage } from "../src/message.js";
import type { ReplicationDomainConstructor } from "../src/replication-domain.js";
import type { SynchronizerConstructor } from "../src/sync/index.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";

export const collectMessages = (log: SharedLog<any, any>) => {
	const messages: [TransportMessage, PublicSignKey][] = [];

	// TODO types
	const onMessage = log.rpc["_responseHandler"];
	log.rpc["_responseHandler"] = (msg: any, ctx: any) => {
		messages.push([msg, ctx.from]);
		return onMessage(msg, ctx);
	};
	return messages;
};

export const collectMessagesFn = (log: SharedLog<any, any>) => {
	const messages: [TransportMessage, PublicSignKey][] = [];
	const onMessageOrg = log.onMessage.bind(log);
	const fn = async (msg: any, ctx: any) => {
		messages.push([msg, ctx.from]);
		onMessageOrg(msg, ctx);
	};
	return { messages, fn };
};

export const slowDownSend = (
	from: ProgramClient,
	to: ProgramClient,
	ms: number | (() => number) = 3000,
) => {
	const pubsub = from.services.pubsub as TopicControlPlane;
	for (const [_key, peer] of pubsub.peers) {
		if (peer.publicKey.equals(to.identity.publicKey)) {
			const writeFn = peer.write.bind(peer);
			peer.write = async (msg, priority) => {
				await delay(typeof ms === "number" ? ms : ms());
				if (peer.rawOutboundStreams?.length > 0) {
					return writeFn(msg, priority);
				}
			};
			return;
		}
	}
	throw new Error("Could not find peer");
};

export const slowDownMessage = (
	log: SharedLog<any, any>,
	type: Constructor<TransportMessage>,
	tms: number,
	abortSignal?: AbortSignal,
) => {
	// TODO types
	const sendFn = log.rpc.send.bind(log.rpc);
	log.rpc.send = async (msg, options) => {
		if (
			msg.constructor?.name === type?.name &&
			abortSignal &&
			abortSignal.aborted === false
		) {
			try {
				await delay(tms, { signal: abortSignal });
			} catch (error) {
				// dont do anything because we might want to send, but immediately
			}
		}
		return sendFn(msg, options);
	};
};

export const getReceivedHeads = (
	messages: [TransportMessage, PublicSignKey][],
): EntryWithRefs<any>[] => {
	const heads: EntryWithRefs<any>[] = [];
	for (const message of messages.filter(
		(x) => x[0] instanceof ExchangeHeadsMessage,
	) as [ExchangeHeadsMessage<any>, PublicSignKey][]) {
		heads.push(...message[0].heads);
	}
	return heads;
};

export const waitForConverged = async (
	fn: () => any,
	options: {
		timeout: number;
		tests: number;
		interval: number;
		delta: number;
		jitter?: number;
		debug?: boolean;
	} = {
		tests: 3,
		delta: 1,
		timeout: 30 * 1000,
		interval: 1000,
		debug: false,
	},
) => {
	let lastResult = undefined;
	let ok = 0;
	const startedAt = Date.now();
	const jitter = options.jitter ?? 0;
	const traceOnTimeout =
		process.env.PEERBIT_TRACE_CONVERGENCE_TIMEOUTS === "1" ||
		process.env.PEERBIT_TRACE_ALL_TEST_FAILURES === "1";
	const samples: number[] = [];
	for (;;) {
		if (Date.now() - startedAt > options.timeout) {
			if (traceOnTimeout) {
				console.error(
					`[converged-timeout] timeoutMs=${options.timeout} intervalMs=${options.interval} delta=${options.delta} jitter=${jitter} tests=${options.tests} last=${String(lastResult)} ok=${ok} samples=${JSON.stringify(samples.slice(-20))}`,
				);
			}
			throw new Error("Timeout");
		}

		const current = await fn();
		samples.push(current);
		if (options.debug) {
			console.log("Waiting for convergence: " + current);
		}

		if (
			lastResult != null &&
			Math.abs(lastResult - current) <= options.delta + jitter
		) {
			ok += 1;
			if (options.tests <= ok) {
				break;
			}
		} else {
			ok = 0;
		}
		lastResult = current;

		if (Date.now() - startedAt > options.timeout) {
			if (traceOnTimeout) {
				console.error(
					`[converged-timeout] timeoutMs=${options.timeout} intervalMs=${options.interval} delta=${options.delta} jitter=${jitter} tests=${options.tests} last=${String(lastResult)} ok=${ok} samples=${JSON.stringify(samples.slice(-20))}`,
				);
			}
			throw new Error("Timeout");
		}

		await delay(options.interval);
	}
};
export const getUnionSize = async (
	dbs: { log: SharedLog<any, any> }[],
	expectedUnionSize: number,
) => {
	const union = new Set<string>();
	for (const db of dbs) {
		for (const value of await db.log.log.toArray()) {
			union.add(value.hash);
		}
	}
	return union.size;
};
export const checkBounded = async (
	entryCount: number,
	lower: number,
	higher: number,
	...dbs: { log: SharedLog<any, any> }[]
) => {
	// Under full-suite load (GC + lots of timers), rebalancing/pruning can take
	// noticeably longer. Use a larger window with slower polling to avoid flaky
	// upper-bound assertions.
	const boundWaitOpts = { timeout: 60_000, delayInterval: 1_000 } as const;

	const checkConverged = async (db: { log: SharedLog<any, any> }) => {
		const a = db.log.log.length;
		await delay(100); // arb delay
		// covergence is when the difference is less than 1% of the max
		return (
			Math.abs(a - db.log.log.length) <
			Math.max(Math.round(Math.max(a, db.log.log.length) * 0.01), 1)
		); // TODO make this a parameter
	};

	for (const db of dbs) {
		try {
			await waitForResolved(() => checkConverged(db), {
				timeout: 25000,
				delayInterval: 2500,
			});
		} catch (error) {
			throw new Error("Log length did not converge");
		}
	}

	await checkReplicas(
		dbs,
		maxReplicas(dbs[0].log, [...(await dbs[0].log.log.toArray())]),
		entryCount,
	);

	for (const db of dbs) {
		try {
			await waitForResolved(
				() => expect(db.log.log.length).greaterThanOrEqual(entryCount * lower),
				boundWaitOpts,
			);
		} catch (error) {
			await dbgLogs(dbs.map((x) => x.log));
			throw new Error(
				"Log did not reach lower bound length of " +
					entryCount * lower +
					" got " +
					db.log.log.length,
			);
		}

		try {
			await waitForResolved(
				() => expect(db.log.log.length).lessThanOrEqual(entryCount * higher),
				boundWaitOpts,
			);
		} catch (error) {
			await dbgLogs(dbs.map((x) => x.log));
			throw new Error(
				"Log did not conform to upper bound length of " +
					entryCount * higher +
					" got " +
					db.log.log.length,
			);
		}
	}
};

export const checkReplicas = async (
	dbs: { log: SharedLog<any, any> }[],
	minReplicas: number,
	entryCount: number,
) => {
	const verboseReplicaTrace = process.env.PEERBIT_TRACE_REPLICA_INVARIANTS === "1";
	let iteration = 0;
	const startedAt = Date.now();

	try {
		// Replica convergence can take longer under full-suite load (GC, many
		// concurrent timers). Use a larger window and slower polling to reduce flakiness
		// and avoid starving the event loop with tight, heavy polling.
		await waitForResolved(
			async () => {
				iteration += 1;
				const map = new Map<string, number>();
				const hashToEntry = new Map<string, Entry<any>>();
				for (const db of dbs) {
					for (const value of await db.log.log.toArray()) {
						// eslint-disable-next-line @typescript-eslint/no-unused-expressions
						expect(await db.log.log.blocks.has(value.hash)).to.be.true;
						map.set(value.hash, (map.get(value.hash) || 0) + 1);
						hashToEntry.set(value.hash, value);
					}
				}

				if (verboseReplicaTrace && map.size > 0) {
					let minObserved = Number.POSITIVE_INFINITY;
					for (const [, count] of map) {
						if (count < minObserved) {
							minObserved = count;
						}
					}
					const elapsedMs = Date.now() - startedAt;
					console.error(
						`[replica-trace] iter=${iteration} elapsedMs=${elapsedMs} entries=${map.size} minExpected=${minReplicas} minObserved=${minObserved}`,
					);
				}

				for (const [_k, v] of map) {
					try {
						expect(v).greaterThanOrEqual(minReplicas);
					} catch {
						const entry = hashToEntry.get(_k)!;
						const gid = entry.meta.gid;
						const coordinates = await dbs[0].log.createCoordinates(
							entry,
							minReplicas,
						);
						const diag = await gatherReplicaDiagnostics(
							dbs,
							entry,
							minReplicas,
							entryCount,
							iteration,
							Date.now() - startedAt,
						);
						throw new Error(
							"Did not fulfill min replicas level for " +
								entry.hash +
								" coordinates" +
								JSON.stringify(coordinates.map((x) => x.toString())) +
								" of: " +
								minReplicas +
								" got " +
								v +
								". Gid to peer history? " +
								JSON.stringify(
									dbs.map(
										(x) =>
											[...(x.log._gidPeersHistory.get(gid) || [])].filter(
												(id) => id !== x.log.node.identity.publicKey.hashcode(),
											).length || 0,
									) +
										". Has? " +
										JSON.stringify(
											await Promise.all(
												dbs.map((x) => x.log.log.has(entry.hash)),
											),
										) +
										", sync in flight ? " +
										JSON.stringify(
											dbs.map((x) =>
												x.log.syncronizer.syncInFlight.has(entry.hash),
											),
										),
								) +
								". Diagnostics: " +
								JSON.stringify(diag),
						);
					}
					expect(v).lessThanOrEqual(dbs.length);
				}
			},
			{ timeout: 120_000, delayInterval: 1_000 },
		);
	} catch (error) {
		await dbgLogs(dbs.map((x) => x.log));
		throw error;
	}
};

const gatherReplicaDiagnostics = async (
	dbs: { log: SharedLog<any, any> }[],
	entry: Entry<any>,
	minReplicas: number,
	entryCount: number,
	iteration: number,
	elapsedMs: number,
) => {
	const gid = entry.meta.gid;
	const coordinates = await dbs[0].log.createCoordinates(entry, minReplicas);

	const perNode = await Promise.all(
		dbs.map(async (db) => {
			const id = db.log.node.identity.publicKey.hashcode();
			const logLength = db.log.log.length;
			const hasEntry = await db.log.log.has(entry.hash);
			const blockHasEntry = await db.log.log.blocks.has(entry.hash);
			const inFlight = db.log.syncronizer.syncInFlight.has(entry.hash);
			const gidPeerHistoryCount =
				[...(db.log._gidPeersHistory.get(gid) || [])].filter(
					(peerId) => peerId !== id,
				).length || 0;
			const segments = (await db.log.getAllReplicationSegments()).map((segment) =>
				segment.toString(),
			);

			let totalParticipation: number | string = "n/a";
			let myParticipation: number | string = "n/a";
			try {
				totalParticipation = await db.log.calculateTotalParticipation();
				myParticipation = await db.log.calculateMyTotalParticipation();
			} catch {
				// Keep diagnostics best-effort while nodes are joining/leaving.
			}

			return {
				id,
				logLength,
				hasEntry,
				blockHasEntry,
				inFlight,
				gidPeerHistoryCount,
				segments,
				totalParticipation,
				myParticipation,
			};
		}),
	);

	return {
		iteration,
		elapsedMs,
		entryCount,
		entryHash: entry.hash,
		gid,
		coordinates: coordinates.map((x) => x.toString()),
		perNode,
	};
};
export type TestSetupConfig<R extends "u32" | "u64"> = {
	type: R;
	domain: ReplicationDomainConstructor<ReplicationDomainHash<R>>;
	syncronizer: SynchronizerConstructor<R>;
	name: string;
};

export const testSetups: TestSetupConfig<any>[] = [
	{
		domain: createReplicationDomainHash("u32"),
		type: "u32",
		syncronizer: SimpleSyncronizer,
		name: "u32-simple",
	},
	{
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: SimpleSyncronizer,
		name: "u64-simple",
	},
	{
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: RatelessIBLTSynchronizer,
		name: "u64-iblt",
	},
];
export const checkIfSetupIsUsed = (
	setup: TestSetupConfig<any>,
	log: SharedLog<any, any, any>,
) => {
	expect(log.domain.type).to.equal(setup.domain(log).type);
	expect(log.syncronizer.constructor).to.equal(setup.syncronizer);
};

export const dbgLogs = async (log: SharedLog<any, any>[]) => {
	for (const l of log) {
		console.error(
			"Id:",
			l.node.identity.publicKey.hashcode(),
			"Log length:",
			l.log.length,
			"Replication segments:",
			// eslint-disable-next-line @typescript-eslint/no-base-to-string
			(await l.getAllReplicationSegments()).map((x) => x.toString()),
			"Prunable: " + (await l.getPrunable()).length,
			"log length: ",
			l.log.length,
		);
	}
};
