/**
 * How long the removal of a replica that pubsub reported unreachable is held
 * before it is queued for the usual placement pass.
 */
export const TRANSPORT_LOSS_REMOVAL_GRACE_MS = 10_000;

/** Why a held removal was queued after all. */
export type TransportLossReleaseReason =
	| "grace"
	| "pass"
	| "changed"
	| "declined";

/**
 * A removal held after a transport loss: the rows it deleted and the
 * ownership state it was held under. Queueing it adds exactly the changes the
 * removal would have queued at once.
 */
export type TransportLossPark<T> = {
	/** The rows the removal deleted. */
	rows: T[];
	/** The same rows by id string. */
	rowsById: Map<string, T>;
	/** The change timestamp the removal would have queued. */
	timestamp: bigint;
	/** The change debouncer that was current when the removal was held. */
	debouncer: object;
	/** The ownership lifecycle that was current when the removal was held. */
	controller: AbortController;
	timer?: ReturnType<typeof setTimeout>;
};

/**
 * True only when the diffs add back exactly the held rows: every diff is an
 * addition, ids are not repeated, and each row is byte-identical to the held
 * row with the same id (owner, timestamp, mode, both intervals and width).
 */
export const rowsMatchTransportLossPark = <
	T extends { idString: string; rangeHash: string },
>(
	park: { rows: T[]; rowsById: Map<string, T> },
	diffs: { type: string; range: T }[],
): boolean => {
	if (diffs.length !== park.rows.length) {
		return false;
	}
	const seen = new Set<string>();
	for (const diff of diffs) {
		const id = diff.range.idString;
		if (diff.type !== "added" || seen.has(id)) {
			return false;
		}
		seen.add(id);
		const held = park.rowsById.get(id);
		if (!held || held.rangeHash !== diff.range.rangeHash) {
			return false;
		}
	}
	return true;
};
