import { PublicSignKey } from "@peerbit/crypto";
import { TransportMessage } from "../message";
import { SharedLog } from "..";
import { EntryWithRefs, ExchangeHeadsMessage } from "../exchange-heads";
import { delay } from "@peerbit/time";
import { Constructor } from "@dao-xyz/borsh";

export const collectMessages = (log: SharedLog<any>) => {
	const messages: [TransportMessage, PublicSignKey][] = [];

	// TODO types
	const onMessage = log.rpc["_responseHandler"];
	log.rpc["_responseHandler"] = (msg, ctx) => {
		messages.push([msg, ctx.from]);
		return onMessage(msg, ctx);
	};
	return messages;
};

export const slowDownSend = (
	log: SharedLog<any>,
	type: Constructor<TransportMessage>,
	tms: number,
	abortSignal?: AbortSignal
) => {
	// TODO types
	const sendFn = log.rpc.send.bind(log.rpc);
	log.rpc.send = async (msg, options) => {
		if (msg.constructor === type) {
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
	messages: [TransportMessage, PublicSignKey][]
): EntryWithRefs<any>[] => {
	const heads: EntryWithRefs<any>[] = [];
	for (const message of messages.filter(
		(x) => x[0] instanceof ExchangeHeadsMessage
	) as [ExchangeHeadsMessage<any>, PublicSignKey][]) {
		heads.push(...message[0].heads);
	}
	return heads;
};

export const waitForConverged = async (
	fn: () => any,
	options: { timeout: number; tests: number } = { tests: 3, timeout: 20 * 1000 }
) => {
	let lastResult = undefined;
	let c = 0;
	let ok = 0;
	for (;;) {
		const current = await fn();
		if (lastResult == current) {
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
