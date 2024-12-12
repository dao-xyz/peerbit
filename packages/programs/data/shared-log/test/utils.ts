import { type Constructor } from "@dao-xyz/borsh";
import type { PublicSignKey } from "@peerbit/crypto";
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
	options: { timeout: number; tests: number; interval: number } = {
		tests: 3,
		timeout: 30 * 1000,
		interval: 1000,
	},
) => {
	let lastResult = undefined;
	let c = 0;
	let ok = 0;
	for (; ;) {
		const current = await fn();
		if (lastResult === current) {
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
	for (const [_i, db] of dbs.entries()) {
		try {
			await waitForResolved(
				() => expect(db.log.log.length).greaterThanOrEqual(entryCount * lower),
				{
					timeout: 25 * 1000,
				},
			);
		} catch (error) {
			const replicationRanges = await Promise.all(
				[...dbs].map((x) => x.log.getAllReplicationSegments()),
			);
			console.error(
				"Log did not reach lower bound length of " +
				entryCount * lower +
				" got " +
				db.log.log.length,
				"Ranges size: ",
				replicationRanges.map((x) => x.length),
			);
			await dbgLogs(dbs.map((x) => x.log));
			throw new Error(
				"Log did not reach lower bound length of " +
				entryCount * lower +
				" got " +
				db.log.log.length,
			);
		}
	}

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

	for (const [_i, db] of dbs.entries()) {
		await waitForResolved(() =>
			expect(db.log.log.length).greaterThanOrEqual(entryCount * lower),
		);
		await waitForResolved(() =>
			expect(db.log.log.length).lessThanOrEqual(entryCount * higher),
		);
	}

	await checkReplicas(
		dbs,
		maxReplicas(dbs[0].log, [...(await dbs[0].log.log.toArray())]),
		entryCount,
	);
};

export const checkReplicas = (
	dbs: { log: SharedLog<any, any> }[],
	minReplicas: number,
	entryCount: number,
) => {
	return waitForResolved(async () => {
		const map = new Map<string, number>();
		for (const db of dbs) {
			for (const value of await db.log.log.toArray()) {
				// eslint-disable-next-line @typescript-eslint/no-unused-expressions
				expect(await db.log.log.blocks.has(value.hash)).to.be.true;
				map.set(value.hash, (map.get(value.hash) || 0) + 1);
			}
		}
		for (const [_k, v] of map) {
			try {
				expect(v).greaterThanOrEqual(minReplicas);
			} catch (error) {
				throw new Error(
					"Did not fulfill min replicas level of: " + minReplicas + " got " + v,
				);
			}
			expect(v).lessThanOrEqual(dbs.length);
		}
	});
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
			(await l.getAllReplicationSegments()).map(x => x.toString()),
			"Prunable: " + (await l.getPrunable()).length,
			"log length: ",
			l.log.length,
			"To prune:",
			l.toPrune?.size,
			"RQ I prune",
			l.requestIPrune?.size,
			"RR",
			l.addedReplciationRangesFrom.size,
			"Received heads",
			l.receivedHeads?.size,
			"Leader heads",
			l.leaderHeads?.size
		);
	}
};
