import { logger as loggerFn } from "@peerbit/logger";

const logger = loggerFn("peerbit:transport:blocks:stored-notifications");

/**
 * Snapshot of the background work that runs `onPut`/`onPutMany` for stored
 * blocks. Counters cover the current open/close generation of the store.
 */
export type StoredNotificationTelemetry = {
	/** CIDs waiting for a hook call. */
	pending: number;
	/** CIDs passed to hook calls that have not settled. */
	running: number;
	/** CIDs accepted for notification. */
	admitted: number;
	/** CIDs skipped because the same CID was already waiting or running. */
	coalesced: number;
	/** CIDs refused because `maxPending` CIDs were already waiting. */
	dropped: number;
	/** Waiting CIDs discarded when the store stopped. */
	abandoned: number;
	limits: {
		/** CIDs that may wait for a hook call. */
		maxPending: number;
		/** Hook calls that may run at the same time. */
		concurrency: number;
	};
};

export type StoredNotificationQueueOptions = {
	maxPending: number;
	/** Concurrent calls when every call notifies one CID. */
	concurrency: number;
	/**
	 * Whether one call may notify every waiting CID (an aggregate hook). Such
	 * calls run one at a time, so CIDs stored meanwhile form the next batch
	 * instead of starting another bootstrap lookup.
	 */
	aggregate: () => boolean;
	notify: (cids: string[]) => Promise<void> | void;
};

type Task = {
	cids: string[];
	/** Admission ticket of the oldest CID in this call. */
	ticket: number;
	settled: Promise<void>;
};

/**
 * Runs best-effort notifications for stored blocks in the background, so a
 * write never waits for a provider announcement.
 *
 * - A CID that is already waiting or running is coalesced.
 * - At most `maxPending` CIDs wait. Later CIDs are dropped and counted, with
 *   one debug log each time the queue fills up.
 * - At most `concurrency` single-CID calls, or one aggregate call, run at once.
 * - `close()` discards waiting CIDs and resolves when running calls settle.
 *   The owner aborts those calls first through the signal it hands its hooks.
 */
export class StoredNotificationQueue {
	private readonly pending = new Map<string, number>();
	private readonly runningCids = new Set<string>();
	private readonly tasks = new Set<Task>();
	private readonly waiters: Array<{ fence: number; resolve: () => void }> = [];
	private readonly counts = {
		admitted: 0,
		coalesced: 0,
		dropped: 0,
		abandoned: 0,
	};
	private lastTicket = 0;
	private pumpScheduled = false;
	private overflowing = false;
	private _closed = false;

	constructor(private readonly options: StoredNotificationQueueOptions) {}

	get closed(): boolean {
		return this._closed;
	}

	enqueue(cids: Iterable<string>): void {
		if (this._closed) return;
		let admitted = false;
		for (const cid of cids) {
			if (!cid) continue;
			if (this.pending.has(cid) || this.runningCids.has(cid)) {
				this.counts.coalesced += 1;
				continue;
			}
			if (this.pending.size >= this.options.maxPending) {
				this.counts.dropped += 1;
				if (!this.overflowing) {
					this.overflowing = true;
					logger(
						"stored-block notification queue is full (%d waiting); dropping notifications until it drains",
						this.pending.size,
					);
				}
				continue;
			}
			// Map insertion order is admission order, so the first waiting CID
			// always holds the oldest waiting ticket.
			this.pending.set(cid, ++this.lastTicket);
			this.counts.admitted += 1;
			admitted = true;
		}
		if (admitted) this.schedulePump();
	}

	/**
	 * Resolves once every CID admitted before this call has been notified
	 * (successfully or not) or discarded by `close()`.
	 */
	whenSettled(): Promise<void> {
		const fence = this.lastTicket;
		if (this.oldestUnsettledTicket() > fence) return Promise.resolve();
		return new Promise<void>((resolve) => {
			this.waiters.push({ fence, resolve });
		});
	}

	close(): Promise<void> {
		if (!this._closed) {
			this._closed = true;
			this.counts.abandoned += this.pending.size;
			this.pending.clear();
			this.releaseWaiters();
		}
		if (this.tasks.size === 0) return Promise.resolve();
		return Promise.all([...this.tasks].map((task) => task.settled)).then(
			(): void => undefined,
		);
	}

	telemetry(): StoredNotificationTelemetry {
		return {
			pending: this.pending.size,
			running: this.runningCids.size,
			...this.counts,
			limits: {
				maxPending: this.options.maxPending,
				concurrency: this.concurrency(),
			},
		};
	}

	private concurrency(): number {
		return this.options.aggregate()
			? 1
			: Math.max(1, Math.floor(this.options.concurrency));
	}

	private schedulePump(): void {
		if (this.pumpScheduled) return;
		this.pumpScheduled = true;
		// A microtask takes hook calls out of the writer's synchronous section
		// and lets CIDs stored together share one aggregate call. It needs no
		// timer, so a fake clock cannot hold announcements back.
		void Promise.resolve().then(() => {
			this.pumpScheduled = false;
			this.pump();
		});
	}

	private pump(): void {
		if (this._closed) return;
		const aggregate = this.options.aggregate();
		const limit = this.concurrency();
		while (this.tasks.size < limit && this.pending.size > 0) {
			const take = aggregate ? this.pending.size : 1;
			const cids: string[] = [];
			let ticket = Number.POSITIVE_INFINITY;
			for (const [cid, cidTicket] of this.pending) {
				this.pending.delete(cid);
				this.runningCids.add(cid);
				cids.push(cid);
				ticket = Math.min(ticket, cidTicket);
				if (cids.length >= take) break;
			}
			this.run(cids, ticket);
		}
		if (this.pending.size < this.options.maxPending) {
			this.overflowing = false;
		}
	}

	private run(cids: string[], ticket: number): void {
		let result: Promise<void> | void;
		try {
			result = this.options.notify(cids);
		} catch (error) {
			logger("stored-block notification failed: %e", error);
			result = undefined;
		}
		const task: Task = { cids, ticket, settled: Promise.resolve() };
		this.tasks.add(task);
		task.settled = Promise.resolve(result).then(
			() => this.finish(task),
			(error: unknown) => {
				logger("stored-block notification failed: %e", error);
				this.finish(task);
			},
		);
	}

	private finish(task: Task): void {
		this.tasks.delete(task);
		for (const cid of task.cids) this.runningCids.delete(cid);
		this.releaseWaiters();
		this.pump();
	}

	private oldestUnsettledTicket(): number {
		let oldest = Number.POSITIVE_INFINITY;
		for (const ticket of this.pending.values()) {
			oldest = ticket;
			break;
		}
		for (const task of this.tasks) {
			if (task.ticket < oldest) oldest = task.ticket;
		}
		return oldest;
	}

	private releaseWaiters(): void {
		if (this.waiters.length === 0) return;
		const oldest = this.oldestUnsettledTicket();
		let released = 0;
		// Fences are taken from a counter that only grows, so waiters are
		// ordered and can be released from the front.
		while (
			released < this.waiters.length &&
			this.waiters[released]!.fence < oldest
		) {
			this.waiters[released]!.resolve();
			released += 1;
		}
		if (released > 0) this.waiters.splice(0, released);
	}
}
