import { field, option, variant, vec } from "@dao-xyz/borsh";
import { LamportClock as Clock } from "./clock.js";
import { EntryType } from "./entry-type.js";

@variant(0)
export class Meta {
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

	equals(other: Meta): boolean {
		return (
			this.gid === other.gid &&
			this.clock.equals(other.clock) &&
			this.clock.id === other.clock.id &&
			this.type === other.type &&
			this.data === other.data &&
			this.next === other.next
		);
	}
}
