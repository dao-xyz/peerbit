import { logger as loggerFn } from "@peerbit/logger";

const logger = loggerFn("peerbit:transport:blocks:stored-notifications");
const warn = logger.newScope("warn");

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
	/**
	 * CIDs refused because their write arrived while `maxPending` or more CIDs
	 * were waiting.
	 */
	dropped: number;
	/** Waiting CIDs discarded when the store stopped. */
	abandoned: number;
	limits: {
		/**
		 * Waiting CIDs at which writes stop being admitted. A write that arrives
		 * below it is admitted whole, so at most `maxPending - 1` CIDs plus the
		 * CIDs of one write wait.
		 */
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
	/**
	 * Calls the hooks. `batch` is true when the CIDs include blocks that one
	 * write stored together, and false when they only come from writes that
	 * each stored a single block.
	 */
	notify: (cids: string[], batch: boolean) => Promise<void> | void;
};

/** Shared by the CIDs that one write admitted. */
type Admission = {
	/** Orders writes for `whenSettled()`. */
	ticket: number;
	/** Whether the write stored more than one block. */
	batch: boolean;
};

type Task = {
	cids: string[];
	/** Ticket of the oldest write in this call. */
	ticket: number;
	settled: Promise<void>;
};

/**
 * Runs best-effort notifications for stored blocks in the background, so a
 * write never waits for a provider announcement.
 *
 * - A CID that is already waiting or running is coalesced.
 * - A write's new CIDs are admitted together while fewer than `maxPending`
 *   CIDs wait, and dropped together (and counted) otherwise, so the bound
 *   never cuts a write off part-way. CIDs of a dropped write that were already
 *   waiting or running are still notified. Each time the queue fills up, its
 *   first drop is logged: in the warn scope the first time (once per store
 *   generation, since a restarted store gets a new queue), at debug after.
 * - At most `concurrency` single-CID calls, or one aggregate call, run at once.
 *   A call holds its slot until it settles. The queue sets no deadline, so
 *   CIDs stored meanwhile wait for it, up to the bound above.
 * - `close()` discards waiting CIDs and resolves when running calls settle.
 *   The owner aborts those calls first through the signal it hands its hooks.
 */
export class StoredNotificationQueue {
	private readonly pending = new Map<string, Admission>();
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
	private warnedOverflow = false;
	private _closed = false;

	constructor(private readonly options: StoredNotificationQueueOptions) {}

	get closed(): boolean {
		return this._closed;
	}

	/** Queues the CIDs that one write stored. */
	enqueue(cids: Iterable<string>): void {
		if (this._closed) return;
		// Decide once per write, so a large write is not cut off part-way. This
		// bounds the waiting CIDs by `maxPending - 1` plus one write, whose CIDs
		// its caller holds anyway.
		const admit = this.pending.size < this.options.maxPending;
		let admission: Admission | undefined;
		let joined: Admission[] | undefined;
		let named = 0;
		let dropped = 0;
		for (const cid of cids) {
			if (!cid) continue;
			named += 1;
			const waiting = this.pending.get(cid);
			if (waiting || this.runningCids.has(cid)) {
				this.counts.coalesced += 1;
				if (waiting && waiting !== admission) (joined ??= []).push(waiting);
				continue;
			}
			if (!admit) {
				dropped += 1;
				continue;
			}
			admission ??= { ticket: ++this.lastTicket, batch: false };
			// Map insertion order is admission order, so the first waiting CID
			// always holds the oldest waiting ticket.
			this.pending.set(cid, admission);
			this.counts.admitted += 1;
		}
		// No pump runs before this synchronous call returns, so the flag still
		// reaches the call that takes these CIDs, including CIDs this write
		// coalesced into an earlier write's waiting ones.
		if (named > 1) {
			if (admission) admission.batch = true;
			for (const earlier of joined ?? []) earlier.batch = true;
		}
		if (admission) this.schedulePump();
		if (dropped > 0) this.recordDropped(dropped);
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

	private recordDropped(count: number): void {
		this.counts.dropped += count;
		if (this.overflowing) return;
		this.overflowing = true;
		// The first overflow goes to the warn scope, which can be enabled
		// without the rest of this module's debug output; later overflows of
		// the same queue stay at debug.
		const log = this.warnedOverflow ? logger : warn;
		this.warnedOverflow = true;
		log(
			"stored-block notification queue is full (%d CIDs waiting); dropping the provider notifications of later writes until it drains (%d dropped so far)",
			this.pending.size,
			this.counts.dropped,
		);
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
			let batch = false;
			for (const [cid, admission] of this.pending) {
				this.pending.delete(cid);
				this.runningCids.add(cid);
				cids.push(cid);
				if (admission.ticket < ticket) ticket = admission.ticket;
				if (admission.batch) batch = true;
				if (cids.length >= take) break;
			}
			this.run(cids, ticket, batch);
		}
		if (this.pending.size < this.options.maxPending) {
			this.overflowing = false;
		}
	}

	private run(cids: string[], ticket: number, batch: boolean): void {
		// Register the call before the hook runs, so a `whenSettled()` or
		// `close()` that the hook reaches synchronously waits for this call too.
		let settle!: () => void;
		const task: Task = {
			cids,
			ticket,
			settled: new Promise<void>((resolve) => {
				settle = resolve;
			}),
		};
		this.tasks.add(task);
		let result: Promise<void> | void;
		try {
			result = this.options.notify(cids, batch);
		} catch (error) {
			logger("stored-block notification failed: %e", error);
			result = undefined;
		}
		const done = () => {
			try {
				this.finish(task);
			} finally {
				settle();
			}
		};
		Promise.resolve(result).then(done, (error: unknown) => {
			logger("stored-block notification failed: %e", error);
			done();
		});
	}

	private finish(task: Task): void {
		this.tasks.delete(task);
		for (const cid of task.cids) this.runningCids.delete(cid);
		this.releaseWaiters();
		this.pump();
	}

	private oldestUnsettledTicket(): number {
		let oldest = Number.POSITIVE_INFINITY;
		for (const admission of this.pending.values()) {
			oldest = admission.ticket;
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
