export class NoPeersError extends Error {
	constructor(topic: string) {
		super(
			`No peers found for topic ${topic}. Please make sure you are connected to the network and try again.`,
		);
	}
}

/**
 * The native transaction committed hot/runtime facts before its entry block
 * could be mirrored to durable storage. Lower-log publication is held behind
 * the mirror barrier, but blindly retrying the user operation is unsafe because
 * native state may already contain the attempted commit.
 */
export class NativeDurableCommitError extends Error {
	readonly nativeCommitApplied = true;
	readonly retrySafe = false;
	readonly cause: unknown;
	readonly committedCids: readonly string[];
	readonly failedCids: readonly string[];
	private readonly committedCidList: string[];
	private readonly failedCidList: string[];

	constructor(
		cause: unknown,
		options?: {
			committedCids?: Iterable<string>;
			failedCids?: Iterable<string>;
		},
	) {
		const committedCids = [...(options?.committedCids ?? [])];
		const failedCids = [...(options?.failedCids ?? committedCids)];
		super(
			`Native commit applied but a durable block mutation failed; automatic retry is unsafe: ${
				cause instanceof Error ? cause.message : String(cause)
			}${failedCids.length > 0 ? ` (failed CIDs: ${failedCids.join(", ")})` : ""}`,
		);
		this.name = "NativeDurableCommitError";
		this.cause = cause;
		this.committedCidList = committedCids;
		this.failedCidList = failedCids;
		this.committedCids = this.committedCidList;
		this.failedCids = this.failedCidList;
	}

	/** @internal Merge facts from other mutations covered by the same poison. */
	addCommitContext(
		options?: {
			committedCids?: Iterable<string>;
			failedCids?: Iterable<string>;
		},
		properties?: { preferIncomingOrder?: boolean },
	): void {
		const merge = (
			target: string[],
			incoming: Iterable<string> | undefined,
		): void => {
			if (!incoming) {
				return;
			}
			const values = properties?.preferIncomingOrder
				? [...incoming, ...target]
				: [...target, ...incoming];
			target.splice(0, target.length, ...new Set(values));
		};
		merge(this.committedCidList, options?.committedCids);
		merge(this.failedCidList, options?.failedCids);
	}
}
