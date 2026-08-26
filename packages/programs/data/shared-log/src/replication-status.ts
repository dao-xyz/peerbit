export const REPLICATION_STATUS_REASONS = [
	"storage-objective-exceeded",
	"range-coverage-underfilled",
	"default-replica-target-unattainable",
] as const;

export type ReplicationStatusReason =
	(typeof REPLICATION_STATUS_REASONS)[number];

export type ReplicationStatusInput = Readonly<{
	storageUsedBytes: number;
	storageObjectiveBytes?: number;
	rangeCoverage: number;
	defaultReplicaTarget: number;
	activeReplicators: number;
}>;

/**
 * Local, advisory replication health. This snapshot is never persisted or
 * exchanged with another peer, and must not be used as placement or pruning
 * authority.
 */
export type ReplicationStatus = Readonly<{
	healthy: boolean;
	reasons: readonly ReplicationStatusReason[];
	storage: Readonly<{
		usedBytes: number;
		objectiveBytes?: number;
		/** `limits.storage` is a PID objective, not an enforced quota. */
		semantics: "soft-objective";
	}>;
	coverage: Readonly<{
		actual: number;
		target: number;
	}>;
	replicas: Readonly<{
		active: number;
		target: number;
	}>;
}>;

/** Classify a detached measurement without reading or mutating runtime state. */
export const classifyReplicationStatus = (
	input: ReplicationStatusInput,
): ReplicationStatus => {
	const reasons: ReplicationStatusReason[] = [];
	if (
		input.storageObjectiveBytes != null &&
		input.storageUsedBytes > input.storageObjectiveBytes
	) {
		reasons.push("storage-objective-exceeded");
	}
	if (input.rangeCoverage < input.defaultReplicaTarget) {
		reasons.push("range-coverage-underfilled");
	}
	if (input.activeReplicators < input.defaultReplicaTarget) {
		reasons.push("default-replica-target-unattainable");
	}

	return Object.freeze({
		healthy: reasons.length === 0,
		reasons: Object.freeze(reasons),
		storage: Object.freeze({
			usedBytes: input.storageUsedBytes,
			objectiveBytes: input.storageObjectiveBytes,
			semantics: "soft-objective" as const,
		}),
		coverage: Object.freeze({
			actual: input.rangeCoverage,
			target: input.defaultReplicaTarget,
		}),
		replicas: Object.freeze({
			active: input.activeReplicators,
			target: input.defaultReplicaTarget,
		}),
	});
};
