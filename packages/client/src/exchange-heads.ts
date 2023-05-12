import {
	variant,
	option,
	field,
	vec,
	serialize,
	fixedArray,
} from "@dao-xyz/borsh";
import { Entry, Identity } from "@dao-xyz/peerbit-log";
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { MaybeSigned } from "@dao-xyz/peerbit-crypto";
import { Program } from "@dao-xyz/peerbit-program";
import { Log } from "@dao-xyz/peerbit-log";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import { TransportMessage } from "./message.js";
import { v4 as uuid } from "uuid";

const logger = loggerFn({ module: "exchange-heads" });

export class MinReplicas {
	get value(): number {
		throw new Error("Not implemented");
	}
}

@variant(0)
export class AbsolutMinReplicas extends MinReplicas {
	_value: number;
	constructor(value: number) {
		super();
		this._value = value;
	}
	get value() {
		return this._value;
	}
}

/**
 * This thing allows use to faster sync since we can provide
 * references that can be read concurrently to
 * the entry when doing Log.fromEntry or Log.fromEntryHash
 */
@variant(0)
export class EntryWithRefs<T> {
	@field({ type: Entry })
	entry: Entry<T>;

	@field({ type: vec(Entry) })
	references: Entry<T>[]; // are some parents to the entry

	constructor(properties: { entry: Entry<T>; references: Entry<T>[] }) {
		this.entry = properties.entry;
		this.references = properties.references;
	}
}

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends TransportMessage {
	@field({ type: "string" })
	programAddress: string; //  TODO do we need this really? (since topics are already addresses, which this message is sent on)

	@field({ type: option("u32") })
	programIndex?: number;

	@field({ type: fixedArray("u8", 32) })
	logId: Uint8Array;

	@field({ type: vec(EntryWithRefs) })
	heads: EntryWithRefs<T>[];

	@field({ type: option(MinReplicas) })
	minReplicas?: MinReplicas;

	@field({ type: fixedArray("u8", 4) })
	reserved: Uint8Array = new Uint8Array(4);

	constructor(props: {
		programIndex?: number;
		programAddress: string;
		logId: Uint8Array;
		heads: EntryWithRefs<T>[];
		minReplicas?: MinReplicas;
	}) {
		super();
		this.id = uuid();
		this.logId = props.logId;
		this.programIndex = props.programIndex;
		this.programAddress = props.programAddress;
		this.heads = props.heads;
		this.minReplicas = props.minReplicas;
	}
}

@variant([0, 1])
export class RequestHeadsMessage extends TransportMessage {
	@field({ type: "string" })
	address: string;

	constructor(props: { topic: string; address: string }) {
		super();
		if (props) {
			this.address = props.address;
		}
	}
}

export const createExchangeHeadsMessage = async (
	log: Log<any>,
	program: Program,
	heads: Entry<any>[],
	includeReferences: boolean,
	identity: Identity | undefined
) => {
	const headsSet = new Set(heads);
	const headsWithRefs = await Promise.all(
		heads.map(async (head) => {
			const refs = !includeReferences
				? []
				: (
						await log.getReferenceSamples(head, {
							pointerCount: 8,
							memoryLimit: 1e6 / heads.length,
						})
				  ) // 1mb total limit split on all heads
						.filter((r) => !headsSet.has(r)); // pick a proportional amount of refs so we can efficiently load the log. TODO should be equidistant for good performance?
			return new EntryWithRefs({
				entry: head,
				references: refs,
			});
		})
	);
	logger.debug(`Send latest heads of '${log.id}'`);
	const message = new ExchangeHeadsMessage({
		logId: log.id,
		programIndex: program._programIndex,
		programAddress: (program.address ||
			program.parentProgram.address)!.toString(),
		heads: headsWithRefs,
	});
	const maybeSigned = new MaybeSigned({ data: serialize(message) });
	let signedMessage: MaybeSigned<any> = maybeSigned;
	if (identity) {
		signedMessage = await signedMessage.sign(identity.sign.bind(identity));
	}

	const decryptedMessage = new DecryptedThing({
		data: serialize(signedMessage),
	}); // TODO encryption?
	return serialize(decryptedMessage);
};
