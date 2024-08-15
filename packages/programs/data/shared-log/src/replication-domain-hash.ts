import { BinaryReader, BinaryWriter } from "@dao-xyz/borsh";
import { sha256 } from "@peerbit/crypto";
import type { ShallowOrFullEntry } from "@peerbit/log";
import { getCoverSet, getSamples } from "./ranges.js";
import type {
	Log,
	ReplicationDomain,
	ReplicationDomainMapper,
} from "./replication-domain.js";
import { SEGMENT_COORDINATE_SCALE } from "./role.js";

export const hashToUniformNumber = (hash: Uint8Array) => {
	const seedNumber = new BinaryReader(
		hash.subarray(hash.length - 4, hash.length),
	).u32();
	return seedNumber / 0xffffffff;
};

export const hashTransformer: ReplicationDomainMapper = async (
	entry: ShallowOrFullEntry<any>,
) => {
	// For a fixed set or members, the choosen leaders will always be the same (address invariant)
	// This allows for that same content is always chosen to be distributed to same peers, to remove unecessary copies

	// Convert this thing we wan't to distribute to 8 bytes so we get can convert it into a u64
	// modulus into an index
	const utf8writer = new BinaryWriter();
	utf8writer.string(entry.meta.gid);
	const seed = await sha256(utf8writer.finalize());

	// convert hash of slot to a number
	const cursor = hashToUniformNumber(seed);
	return cursor;
};

export const distribution = getSamples;

export const ReplicationDomainHash: ReplicationDomain<undefined> = {
	mapper: hashTransformer,
	distribute: distribution,
	collect: async (log: Log, roleAge: number | undefined) => {
		roleAge = roleAge ?? (await log.getDefaultMinRoleAge());
		/*  if (this.closed === true) {
             throw new ClosedError();
         } */

		// Total replication "width"
		const width = 1;

		// How much width you need to "query" to
		const minReplicas = Math.min(
			await log.replicationIndex.getSize(),
			log.replicas.min.getValue(log),
		);

		// If min replicas = 2
		// then we need to make sure we cover 0.5 of the total 'width' of the replication space
		// to make sure we reach sufficient amount of nodes such that at least one one has
		// the entry we are looking for
		const coveringWidth = width / minReplicas;

		let widthToCoverScaled = coveringWidth * SEGMENT_COORDINATE_SCALE;

		const set = await getCoverSet(
			log.replicationIndex,
			roleAge,
			log.node.identity.publicKey,
			widthToCoverScaled,
			SEGMENT_COORDINATE_SCALE,
		);

		// add all in flight
		for (const [key, _] of log.syncInFlight) {
			set.add(key);
		}

		return [...set];
	},
};
