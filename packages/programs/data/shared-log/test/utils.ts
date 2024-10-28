import { type Constructor } from "@dao-xyz/borsh";
import type { PublicSignKey } from "@peerbit/crypto";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	type EntryWithRefs,
	ExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { type SharedLog, maxReplicas } from "../src/index.js";
import type { TransportMessage } from "../src/message";

export const collectMessages = (log: SharedLog<any>) => {
	const messages: [TransportMessage, PublicSignKey][] = [];

	// TODO types
	const onMessage = log.rpc["_responseHandler"];
	log.rpc["_responseHandler"] = (msg: any, ctx: any) => {
		messages.push([msg, ctx.from]);
		return onMessage(msg, ctx);
	};
	return messages;
};

export const collectMessagesFn = (log: SharedLog<any>) => {
	const messages: [TransportMessage, PublicSignKey][] = [];
	const onMessageOrg = log._onMessage.bind(log);
	const fn = async (msg: any, ctx: any) => {
		messages.push([msg, ctx.from]);
		onMessageOrg(msg, ctx);
	};
	return { messages, fn };
};

export const slowDownSend = (
	log: SharedLog<any>,
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
	options: { timeout: number; tests: number } = {
		tests: 3,
		timeout: 30 * 1000,
	},
) => {
	let lastResult = undefined;
	let c = 0;
	let ok = 0;
	for (;;) {
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
		await delay(1000);
		c++;
		if (c * 1000 > options.timeout) {
			throw new Error("Timeout");
		}
	}
};
export const getUnionSize = async (
	dbs: { log: SharedLog<any> }[],
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
	...dbs: { log: SharedLog<any> }[]
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
			throw new Error(
				"Log did not reach lower bound length of " +
					entryCount * lower +
					" got " +
					db.log.log.length,
			);
		}
	}

	const checkConverged = async (db: { log: SharedLog<any> }) => {
		const a = db.log.log.length;
		await delay(100); // arb delay
		return a === db.log.log.length;
	};

	for (const [_i, db] of dbs.entries()) {
		await waitFor(() => checkConverged(db), {
			timeout: 25000,
			delayInterval: 2500,
		});
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
	dbs: { log: SharedLog<any> }[],
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
