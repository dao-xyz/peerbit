import { NotStartedError as IndexNotStartedError } from "@peerbit/indexer-interface";
import { ClosedError } from "@peerbit/program";
import { NotStartedError } from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";

export const isNotStartedError = (e: Error) => {
	if (e instanceof AbortError) {
		return true;
	}
	if (e instanceof NotStartedError) {
		return true;
	}
	if (e instanceof IndexNotStartedError) {
		return true;
	}
	if (e instanceof ClosedError) {
		return true;
	}
	return false;
};

export class NoPeersError extends Error {
	constructor(topic: string) {
		super(
			`No peers found for topic ${topic}. Please make sure you are connected to the network and try again.`,
		);
	}
}

/**
 * A persisted-delivery request failed after the entries were committed locally.
 * Retrying the original append can therefore create a second logical write.
 */
export class PersistedDeliveryError extends Error {
	readonly localCommitSucceeded = true;
	readonly retrySafe = false;
	readonly cause: unknown;
	readonly committedHashes: readonly string[];

	constructor(cause: unknown, committedHashes: Iterable<string>) {
		const previous =
			cause instanceof PersistedDeliveryError ? cause.committedHashes : [];
		const rootCause =
			cause instanceof PersistedDeliveryError ? cause.cause : cause;
		const hashes = [...new Set([...previous, ...committedHashes])];
		const displayedHashes = hashes.slice(0, 8);
		const omitted = hashes.length - displayedHashes.length;
		super(
			`Local commit succeeded, but persisted delivery failed; automatic retry is unsafe: ${
				rootCause instanceof Error ? rootCause.message : String(rootCause)
			}${
				displayedHashes.length > 0
					? ` (committed hashes: ${displayedHashes.join(", ")}${
							omitted > 0 ? `, +${omitted} more` : ""
						})`
					: ""
			}`,
		);
		this.name = "PersistedDeliveryError";
		this.cause = rootCause;
		this.committedHashes = hashes;
	}
}

/**
 * The `compatibility` open option was removed: the pre-v10 replication-info
 * network compatibility modes are retired. Any explicitly provided value —
 * including 10, which previously behaved like the default — rejects before
 * open() performs any side effect. Omit the option (or pass `undefined`) to
 * open in the only supported mode. Document-level `compatibility: 6|7` mapped
 * onto this option and is rejected at the document layer with its own error.
 */
export class CompatibilityModeRetiredError extends Error {
	constructor(value: unknown) {
		super(
			`The SharedLog "compatibility" open option was removed: replication-info network compatibility modes are retired. ` +
				`Received explicit value ${String(value)}; remove the option to open this log ` +
				`(documents previously used compatibility 6/7, which mapped onto this option and is rejected at the document layer).`,
		);
		this.name = "CompatibilityModeRetiredError";
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
