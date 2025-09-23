import type { PublicSignKey } from "@peerbit/crypto";
import type { PeerRefs } from "./keys.js";
import type { DataMessage, Message } from "./messages.js";

export * from "./keys.js";

export interface PeerEvents {
	"peer:session": CustomEvent<PublicSignKey>;
	"peer:reachable": CustomEvent<PublicSignKey>;
	"peer:unreachable": CustomEvent<PublicSignKey>;
}

export interface MessageEvents {
	message: CustomEvent<Message>;
}
export interface StreamEvents extends PeerEvents, MessageEvents {
	data: CustomEvent<DataMessage>;
}

export * from "./messages.js";

// ---------- wait for peer types ----------
export type Target = "neighbor" | "reachable";
export type Settle = "any" | "all";

export interface WaitForBaseOpts {
	settle?: Settle; // default: "any"
	timeout?: number; // ms
	signal?: AbortSignal;
	allowSelf?: boolean; // default: false
}

// Special-case overload: seek="present" => target is implicitly "neighbor"
export interface WaitForPresentOpts extends WaitForBaseOpts {
	seek: "present";
	target?: "neighbor";
	// target is intentionally omitted here (always "neighbor")
}

// General-case overload: seek omitted or "any" => target must be explicit (defaults to "reachable")
export interface WaitForAnyOpts extends WaitForBaseOpts {
	seek?: "any";
	target?: Target; // default: "reachable"
}
export type WaitForPeersFn = (
	peer: PeerRefs,
	options?: WaitForPresentOpts | WaitForAnyOpts,
) => Promise<string[]>;
export interface WaitForPeer {
	waitFor(
		peer: PeerRefs,
		options?: WaitForPresentOpts | WaitForAnyOpts,
	): Promise<string[]>;
}

export interface PublicKeyFromHashResolver {
	getPublicKey(
		hash: string,
	): PublicSignKey | undefined | Promise<PublicSignKey | undefined>;
}

export class NotStartedError extends Error {
	constructor() {
		super("Not started");
	}
}

export class DeliveryError extends Error {}
export class InvalidMessageError extends Error {}
