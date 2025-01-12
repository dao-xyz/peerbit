import { BinaryWriter } from "@dao-xyz/borsh";
import { sha256 } from "@peerbit/crypto";
import type { ShallowOrFullEntry } from "@peerbit/log";
import { bytesToNumber } from "./integers.js";
import { type EntryReplicated } from "./ranges.js";
import {
	type Log,
	type ReplicationDomain,
	type ReplicationDomainConstructor,
	type ReplicationDomainMapper,
} from "./replication-domain.js";

const hashTransformer = <R extends "u32" | "u64">(
	resolution: R,
): ReplicationDomainMapper<any, R> => {
	const numberConverter = bytesToNumber(resolution);
	if (resolution === "u32") {
		return (async (entry: ShallowOrFullEntry<any> | EntryReplicated<R>) => {
			const utf8writer = new BinaryWriter();
			utf8writer.string(entry.meta.gid);
			const seed = await sha256(utf8writer.finalize());
			return numberConverter(seed);
		}) as ReplicationDomainMapper<any, R>;
	} else if (resolution === "u64") {
		return (async (entry: ShallowOrFullEntry<any> | EntryReplicated<R>) => {
			const utf8writer = new BinaryWriter();
			utf8writer.string(entry.meta.gid);
			const seed = await sha256(utf8writer.finalize());
			return numberConverter(seed);
		}) as ReplicationDomainMapper<any, R>;
	} else {
		throw new Error("Unsupported resolution");
	}
};

export type ReplicationDomainHash<R extends "u32" | "u64"> = ReplicationDomain<
	undefined,
	any,
	R
>;

export const createReplicationDomainHash =
	<R extends "u32" | "u64">(
		resolution: R,
	): ReplicationDomainConstructor<ReplicationDomainHash<R>> =>
	(log: Log) => {
		return {
			resolution,
			type: "hash",
			fromEntry: hashTransformer<R>(resolution),
			fromArgs: async (args: undefined) => {
				return {
					offset: log.node.identity.publicKey,
				};
			},
		};
	};
