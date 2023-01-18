import { field, variant, vec, option } from "@dao-xyz/borsh";
import { MaybeSigned, PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { TransportMessage } from "./message.js";
/* 
@variant([2, 0])
export class OpenEvent extends TransportMessage {
	@field({ type: vec("string") })
	addresses: string[];

	constructor(addresses: string[]) {
		super();
		this.addresses = addresses;
	}
}

@variant([2, 1])
export class CloseEvent extends TransportMessage {
	@field({ type: vec("string") })
	addresses: string[];

	@field({ type: option(MaybeSigned) })
	delegation?: MaybeSigned<Delegation>; // Allow this even to be emitted by an observing peer that is trusted by the peer that is shutting down

	constructor(addresses: string[], delegation?: MaybeSigned<Delegation>) {
		super();
		this.addresses = addresses;
		this.delegation = delegation;
	}
}

export class Delegation {
	@field({ type: PublicSignKey })
	to: PublicSignKey;

	constructor(to: PublicSignKey) {
		this.to = to;
	}
}

// when I join a network how do I collect stats about replication?

@variant([2, 2])
export class RequestReplicationInfo extends TransportMessage {
	@field({ type: vec("string") })
	addresses: string[];

	constructor(addresses: string[]) {
		super();
		this.addresses = addresses;
	}
}
 */