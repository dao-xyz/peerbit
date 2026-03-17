import { type PeerId, isPeerId } from "@libp2p/interface";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";

export type PeerRef = PeerId | PublicSignKey | string;
export type PeerRefs =
	| PeerRef
	| PeerRef[]
	| Set<string>
	| IterableIterator<PeerRef>;

const isPublicSignKeyLike = (value: unknown): value is PublicSignKey =>
	value instanceof PublicSignKey ||
	(!!value &&
		typeof value === "object" &&
		typeof (value as PublicSignKey).hashcode === "function" &&
		(value as PublicSignKey).bytes instanceof Uint8Array);

export const coercePeerRefToHash = (to: PeerRef) => {
	return isPublicSignKeyLike(to)
		? to.hashcode()
		: typeof to === "string"
			? to
			: getPublicKeyFromPeerId(to).hashcode();
};

export const coercePeerRefsToHashes = (tos: PeerRefs) => {
	if (
		isPeerId(tos) ||
		isPublicSignKeyLike(tos) ||
		typeof tos === "string"
	) {
		return [coercePeerRefToHash(tos)];
	}
	if (tos instanceof Set) {
		return Array.from(tos).map(coercePeerRefToHash);
	}
	if (tos instanceof Array) {
		return tos.map(coercePeerRefToHash);
	}

	const toHashes: string[] = [];
	for (const to of tos) {
		const hash = coercePeerRefToHash(to);
		toHashes.push(hash);
	}
	return toHashes;
};
