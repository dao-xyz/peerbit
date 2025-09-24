import { type PeerId, isPeerId } from "@libp2p/interface";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";

export type PeerRef = PeerId | PublicSignKey | string;
export type PeerRefs =
	| PeerRef
	| PeerRef[]
	| Set<string>
	| IterableIterator<PeerRef>;

export const coercePeerRefToHash = (to: PeerRef) => {
	return to instanceof PublicSignKey
		? to.hashcode()
		: typeof to === "string"
			? to
			: getPublicKeyFromPeerId(to).hashcode();
};

export const coercePeerRefsToHashes = (tos: PeerRefs) => {
	if (
		isPeerId(tos) ||
		tos instanceof PublicSignKey ||
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
