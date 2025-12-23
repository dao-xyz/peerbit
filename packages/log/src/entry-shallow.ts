import { field, option, variant, vec } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import { LamportClock as Clock } from "./clock.js";
import type { EntryType } from "./entry-type.js";

@variant(0)
export class ShallowMeta {
	@field({ type: Clock })
	clock: Clock;

	@field({ type: "string" })
	gid: string; // graph id

	@field({ type: vec("string") })
	next: string[];

	@field({ type: "u8" })
	type: EntryType;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array; // Optional metadata

	constructor(properties: {
		gid: string;
		clock: Clock;
		type: EntryType;
		data?: Uint8Array;
		next: string[];
	}) {
		this.gid = properties.gid;
		this.clock = properties.clock;
		this.type = properties.type;
		this.data = properties.data;
		this.next = properties.next;
	}
}

@variant(0)
export class ShallowEntry {
	@id({ type: "string" })
	hash: string;

	@field({ type: ShallowMeta })
	meta: ShallowMeta;

	@field({ type: "u32" })
	payloadSize: number;

	@field({ type: "bool" })
	head: boolean;

	constructor(properties: {
		hash: string;
		meta: ShallowMeta;
		payloadSize: number;
		head: boolean;
	}) {
		this.hash = properties.hash;
		this.meta = properties.meta;
		this.payloadSize = properties.payloadSize;
		this.head = properties.head;
	}
}
