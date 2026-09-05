export type DocumentBatchCommittedItem = Readonly<{
	/** Position in the input array captured when putMany was invoked. */
	index: number;
	hash: string;
}>;

export type DocumentBatchLocalCommit =
	| "not-started"
	| "committed"
	| "indeterminate";

/**
 * Failure of a putMany invocation with batching: "required". Local append
 * evidence does not prove remote receipt, document projection completion, or
 * an atomic storage transaction. An indeterminate outcome requires recovery
 * and must never be treated as an empty committed set that is safe to retry.
 */
export class DocumentBatchCommitError extends Error {
	readonly cause: unknown;
	readonly localCommit: DocumentBatchLocalCommit;
	readonly committedItems: readonly DocumentBatchCommittedItem[];
	/** Safety of replaying this local append only; not application side effects. */
	readonly retrySafe: boolean;
	/** Unresolved local append outcome; false does not rule out projection repair. */
	readonly recoveryRequired: boolean;

	constructor(
		cause: unknown,
		localCommit: DocumentBatchLocalCommit,
		committedItems: readonly DocumentBatchCommittedItem[],
	) {
		super(
			`Required document batch failed (${localCommit}): ${cause instanceof Error ? cause.message : String(cause)}`,
		);
		this.name = "DocumentBatchCommitError";
		this.cause = cause;
		this.localCommit = localCommit;
		this.committedItems = Object.freeze(
			committedItems.map(({ index, hash }) => Object.freeze({ index, hash })),
		);
		this.retrySafe = localCommit === "not-started";
		this.recoveryRequired = localCommit === "indeterminate";
		Object.freeze(this);
	}
}
