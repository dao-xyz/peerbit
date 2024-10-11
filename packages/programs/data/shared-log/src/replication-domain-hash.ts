import { BinaryReader, BinaryWriter } from "@dao-xyz/borsh";
import { sha256 } from "@peerbit/crypto";
import type { ShallowOrFullEntry } from "@peerbit/log";
import type { EntryReplicated } from "./ranges.js";
import {
	type Log,
	type ReplicationDomain,
	type ReplicationDomainMapper,
} from "./replication-domain.js";

export const hashToU32 = (hash: Uint8Array) => {
	const seedNumber = new BinaryReader(
		hash.subarray(hash.length - 4, hash.length),
	).u32();
	return seedNumber;
};

const hashTransformer: ReplicationDomainMapper<any> = async (
	entry: ShallowOrFullEntry<any> | EntryReplicated,
) => {
	// For a fixed set or members, the choosen leaders will always be the same (address invariant)
	// This allows for that same content is always chosen to be distributed to same peers, to remove unecessary copies

	// Convert this thing we wan't to distribute to 8 bytes so we get can convert it into a u64
	// modulus into an index
	const utf8writer = new BinaryWriter();
	utf8writer.string(entry.meta.gid);
	const seed = await sha256(utf8writer.finalize());

	// convert hash of slot to a number
	return hashToU32(seed);
};

export type ReplicationDomainHash = ReplicationDomain<undefined, any>;
export const createReplicationDomainHash: () => ReplicationDomainHash = () => {
	return {
		type: "hash",
		fromEntry: hashTransformer,
		fromArgs: async (args: undefined, log: Log) => {
			return {
				offset: log.node.identity.publicKey,
			};
		},
	};
};
