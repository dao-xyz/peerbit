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
	/** What the held owner may be missing; sent to it if the hold is restored. */
	ledger: TransportLossLedger;
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

/** The most hashes a ledger keeps; the oldest are dropped first. */
export const TRANSPORT_LOSS_LEDGER_LIMIT = 1024;

/**
 * What an owner whose transport-loss removal is under way or held may be
 * missing: its unconfirmed append deliveries, then the hashes appended or
 * admitted here since its removal started, oldest first. A restore sends them
 * to the owner; any other end of the hold drops them, because the usual passes
 * then deliver. When full, the oldest hash is dropped and counted; the
 * restore's authoritative sweep still covers it.
 */
export type TransportLossLedger = {
	hashes: Set<string>;
	/**
	 * Times a hash was dropped because the ledger was full. A hash recorded
	 * again after it was dropped, as each entry of a batch larger than the
	 * ledger is, can be dropped and counted again.
	 */
	evicted: number;
	/** Removals of this owner that opened the ledger and have not returned. */
	openers: number;
};

export const createTransportLossLedger = (): TransportLossLedger => ({
	hashes: new Set(),
	evicted: 0,
	openers: 0,
});

/** Record a hash once; when the ledger is full, drop the oldest first. */
export const noteTransportLossLedger = (
	ledger: TransportLossLedger,
	hash: string,
	limit: number,
): void => {
	if (ledger.hashes.has(hash)) return;
	if (limit < 1) {
		ledger.evicted++;
		return;
	}
	while (ledger.hashes.size >= limit) {
		const oldest = ledger.hashes.values().next().value as string;
		ledger.hashes.delete(oldest);
		ledger.evicted++;
	}
	ledger.hashes.add(hash);
};
