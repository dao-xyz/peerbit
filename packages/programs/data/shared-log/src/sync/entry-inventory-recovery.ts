export type EntryInventoryPass = {
	/** One bounded page. Release page buffers before resolving `more`. */
	next(): Promise<"more" | "complete" | "retry">;
	close(): void | Promise<void>;
};

type Job<Session> = {
	session: Session;
	controller: AbortController;
	pass?: EntryInventoryPass;
};

/**
 * FIFO page turns: known peers may retain constant-state idle cursors, never
 * parked page buffers. Only two page operations run at once. A timed-out
 * physical operation keeps its slot until it settles, including after abort.
 */
export class EntryInventoryRecovery<Session extends object> {
	private readonly pending = new Map<
		string,
		{ job: Job<Session>; after: number }
	>();
	private readonly active = new Map<string, Job<Session>>();
	private timer?: ReturnType<typeof setTimeout>;
	private stopped = false;

	constructor(
		private readonly deps: {
			signal: AbortSignal;
			isCurrent(peer: string, session: Session): boolean;
			create(
				peer: string,
				session: Session,
				signal: AbortSignal,
			): EntryInventoryPass;
			onError(error: unknown): void;
		},
	) {
		deps.signal.addEventListener("abort", this.close, { once: true });
		if (deps.signal.aborted) this.close();
	}

	wake(peer: string, session: Session) {
		if (this.stopped || !this.deps.isCurrent(peer, session)) return;
		const running = this.active.get(peer);
		if (running?.session === session) return;
		const queued = this.pending.get(peer);
		// A duplicate writable signal must not bypass a queued retry's backoff.
		if (queued?.job.session === session) return;
		if (queued) this.retireIdle(queued.job);
		running?.controller.abort();
		this.pending.set(peer, { job: this.newJob(session), after: 0 });
		this.pump();
	}

	forget(peer: string) {
		const queued = this.pending.get(peer);
		this.pending.delete(peer);
		if (queued) this.retireIdle(queued.job);
		this.active.get(peer)?.controller.abort();
		this.pump();
	}

	restartActive() {
		if (this.stopped) return;
		// Parked cursors capture the same ownership generation as active pages.
		for (const [peer, queued] of this.pending) {
			this.retireIdle(queued.job);
			this.pending.set(peer, {
				job: this.newJob(queued.job.session),
				after: queued.after,
			});
		}
		for (const [peer, job] of this.active) {
			if (!this.pending.has(peer))
				this.pending.set(peer, { job: this.newJob(job.session), after: 0 });
			job.controller.abort();
		}
		this.pump();
	}

	close = () => {
		this.stopped = true;
		this.deps.signal.removeEventListener("abort", this.close);
		clearTimeout(this.timer);
		this.timer = undefined;
		for (const { job } of this.pending.values()) this.retireIdle(job);
		this.pending.clear();
		for (const { controller } of this.active.values()) controller.abort();
	};

	private newJob(session: Session): Job<Session> {
		return { session, controller: new AbortController() };
	}

	private report(error: unknown) {
		try {
			void Promise.resolve(this.deps.onError(error)).catch(() => {});
		} catch {
			/* Diagnostics never own recovery. */
		}
	}

	private async closePass(job: Job<Session>) {
		const pass = job.pass;
		job.pass = undefined;
		try {
			await pass?.close();
		} catch (error) {
			this.report(error);
		}
	}

	private retireIdle(job: Job<Session>) {
		job.controller.abort();
		// No page is executing. Abort releases the idle cursor immediately; own
		// asynchronous backend cleanup without creating another physical request.
		void this.closePass(job);
	}

	private pump() {
		clearTimeout(this.timer);
		this.timer = undefined;
		if (this.stopped) return;
		let next = Infinity;
		for (const [peer, queued] of this.pending) {
			if (!this.deps.isCurrent(peer, queued.job.session)) {
				this.pending.delete(peer);
				this.retireIdle(queued.job);
				continue;
			}
			if (this.active.has(peer)) continue;
			if (this.active.size >= 2) break;
			if (queued.after > Date.now()) {
				next = Math.min(next, queued.after);
				continue;
			}
			this.pending.delete(peer);
			const job = queued.job;
			this.active.set(peer, job);
			void this.run(peer, job);
		}
		if (next !== Infinity && this.active.size < 2) {
			this.timer = setTimeout(
				() => this.pump(),
				Math.max(1, next - Date.now()),
			);
		}
	}

	private async run(peer: string, job: Job<Session>) {
		let result: "more" | "complete" | "retry" = "retry";
		try {
			job.pass ??= this.deps.create(peer, job.session, job.controller.signal);
			result = await job.pass.next();
		} catch (error) {
			if (!job.controller.signal.aborted && !this.stopped) {
				this.report(error);
			}
		} finally {
			const canContinue = () =>
				!this.stopped &&
				!job.controller.signal.aborted &&
				!this.pending.has(peer) &&
				this.deps.isCurrent(peer, job.session);
			const keepPass = result === "more" && canContinue();
			if (!keepPass) await this.closePass(job);
			this.active.delete(peer);
			if (keepPass) {
				// The completed page and its physical work are gone before this
				// constant-state cursor returns to the end of the peer queue.
				this.pending.set(peer, { job, after: 0 });
			} else if (result !== "complete" && canContinue()) {
				this.pending.set(peer, {
					job: this.newJob(job.session),
					after: Date.now() + 1000,
				});
			}
			if (keepPass) {
				// A page with no eligible entries may resolve entirely in microtasks.
				// Yield before its continuation so transport events and cancellation
				// timers can run even when every local inventory read is immediate.
				clearTimeout(this.timer);
				this.timer = setTimeout(() => this.pump(), 0);
			} else {
				this.pump();
			}
		}
	}
}
