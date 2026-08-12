import type { ReplicationRangeMessage } from "./ranges.js";

/**
 * Neutral internal representation of a local replication-state mutation.
 *
 * Producers (replicate/unreplicate/join and their corrective snapshot paths)
 * hand these plain tagged objects to the announcement coordinator, which maps
 * them onto the directed V2 wire messages:
 *
 * - `full`    -> FullReplicationInfoV2Message (authoritative snapshot)
 * - `added`   -> AddedReplicationInfoV2Message (incremental delta)
 * - `stopped` -> StoppedReplicationInfoV2Message (retired segment ids)
 *
 * The retired legacy announcement classes (AllReplicatingSegmentsMessage,
 * AddedReplicationSegmentMessage, StoppedReplicating) remain registered
 * decode tombstones on the wire surface but are no longer the internal
 * currency between mutation producers and the V2 sender.
 */
export type FullReplicationInfoMutation = {
	full: { segments: ReplicationRangeMessage<any>[] };
};

export type AddedReplicationInfoMutation = {
	added: { segments: ReplicationRangeMessage<any>[] };
};

export type StoppedReplicationInfoMutation = {
	stopped: { segmentIds: Uint8Array[] };
};

export type ReplicationInfoMutation =
	| FullReplicationInfoMutation
	| AddedReplicationInfoMutation
	| StoppedReplicationInfoMutation;
