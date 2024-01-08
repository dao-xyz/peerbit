import { PublicSignKey } from "@peerbit/crypto";
import { TransportMessage } from "../message";
import { SharedLog } from "..";
import { EntryWithRefs, ExchangeHeadsMessage } from "../exchange-heads";

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
