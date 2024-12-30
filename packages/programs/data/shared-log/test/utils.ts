import { type Constructor } from "@dao-xyz/borsh";
import type { PublicSignKey } from "@peerbit/crypto";
import type { Entry } from "@peerbit/log";
import type { ProgramClient } from "@peerbit/program";
import type { DirectSub } from "@peerbit/pubsub";
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
import type { TransportMessage } from "../src/message";
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
	const onMessageOrg = log._onMessage.bind(log);
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
	const directsub = from.services.pubsub as DirectSub;
	for (const [_key, peer] of directsub.peers) {
		if (peer.publicKey.equals(to.identity.publicKey)) {
			const writeFn = peer.write.bind(peer);
			peer.write = async (msg, priority) => {
				await delay(typeof ms === "number" ? ms : ms());
				if (peer.outboundStream) {
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
	let c = 0;
	let ok = 0;
	for (;;) {
		const current = await fn();
		if (options.debug) {
			console.log("Waiting for convergence: " + current);
		}

		if (lastResult != null && Math.abs(lastResult - current) <= options.delta) {
			ok += 1;
			if (options.tests <= ok) {
				break;
			}
		} else {
			ok = 0;
		}
		lastResult = current;
		await delay(options.interval);
		c++;
		if (c * options.interval > options.timeout) {
			throw new Error("Timeout");
		}
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
	const checkConverged = async (db: { log: SharedLog<any, any> }) => {
		const a = db.log.log.length;
		await delay(100); // arb delay
		// covergence is when the difference is less than 1% of the max
		return (
			Math.abs(a - db.log.log.length) <
			Math.max(Math.round(Math.max(a, db.log.log.length) * 0.01), 1)
		); // TODO make this a parameter
	};

	for (const [_i, db] of dbs.entries()) {
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

	for (const [_i, db] of dbs.entries()) {
		try {
			await waitForResolved(() =>
				expect(db.log.log.length).greaterThanOrEqual(entryCount * lower),
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
			await waitForResolved(() =>
				expect(db.log.log.length).lessThanOrEqual(entryCount * higher),
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
	try {
		await waitForResolved(async () => {
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
			for (const [_k, v] of map) {
				try {
					expect(v).greaterThanOrEqual(minReplicas);
				} catch (error) {
					const entry = hashToEntry.get(_k)!;
					const gid = entry.meta.gid;
					const coordinates = await dbs[0].log.createCoordinates(
						entry,
						minReplicas,
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
							),
					);
				}
				expect(v).lessThanOrEqual(dbs.length);
			}
		});
	} catch (error) {
		await dbgLogs(dbs.map((x) => x.log));
		throw error;
	}
};

export const generateTestsFromResolutions = (
	fn: (domain: ReplicationDomainHash<"u32" | "u64">) => void,
) => {
	const resolutions = ["u32", "u64"] as const;
	for (const resolution of resolutions) {
		describe(resolution, () => {
			fn(createReplicationDomainHash(resolution));
		});
	}
};

export type TestSetupConfig<R extends "u32" | "u64"> = {
	type: R;
	domain: ReplicationDomainHash<R>;
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
	expect(log.domain).to.equal(setup.domain);
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
