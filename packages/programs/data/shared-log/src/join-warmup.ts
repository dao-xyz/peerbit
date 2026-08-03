const JOIN_WARMUP_SEND_SPACING_MS = 250;

export type JoinWarmupSendState<E> = {
	bypassKnownPeerHints: boolean;
	entries: Map<string, E>;
	generation: object;
	lastCompletedAt: number;
	pending: boolean;
	running: boolean;
};

export type JoinWarmupRetryTimer = {
	handle: ReturnType<typeof setTimeout>;
	resolve?: () => void;
};

export type JoinWarmupScheduledRetryBatch<E> = {
	bypassKnownPeerHints: boolean;
	entries: Map<string, E>;
	remainingAttempts: number;
};

export type JoinWarmupScheduledRetryCohort<E> = {
	batches: JoinWarmupScheduledRetryBatch<E>[];
	dueAt: number;
};

export type JoinWarmupScheduledRetrySlot<E> = {
	cohorts: JoinWarmupScheduledRetryCohort<E>[];
	head: number;
	timer?: JoinWarmupRetryTimer;
	timerDueAt?: number;
};

export type JoinWarmupScheduledRetries<E> = {
	generation: object;
	slotsByDelay: Map<number, JoinWarmupScheduledRetrySlot<E>>;
};

export type JoinWarmupDeps<E> = {
	isLifecycleActive: (controller: AbortController) => boolean;
	getCurrentLifecycleController: () => AbortController;
	getRepairRetryTimers: () => Set<ReturnType<typeof setTimeout>>;
	isClosed: () => boolean;
	onTargetCancelled: (target: string) => void;
	bumpSimpleFallbackPasses: () => void;
	sendEntriesSimple: (
		target: string,
		entries: ReadonlyMap<string, E>,
		options: {
			bypassKnownPeers: boolean;
			bypassRecentKnownPeers: boolean;
			isStillCurrent: () => boolean;
			signal: AbortSignal;
		},
	) => Promise<void>;
	logError: (error: any) => void;
};

export class JoinWarmupCoordinator<E> {
	_joinWarmupGenerationByTarget!: Map<string, object>;
	_joinWarmupSendStateByTarget!: Map<string, JoinWarmupSendState<E>>;
	_joinWarmupRetryTimersByTarget!: Map<string, Set<JoinWarmupRetryTimer>>;
	_joinWarmupScheduledRetriesByTarget!: Map<
		string,
		JoinWarmupScheduledRetries<E>
	>;
	_repairSweepJoinWarmupGenerationByTarget!: Map<string, object>;

	constructor(private readonly deps: JoinWarmupDeps<E>) {
		this.reset();
	}

	reset() {
		this._joinWarmupGenerationByTarget = new Map();
		this._joinWarmupSendStateByTarget = new Map();
		this._joinWarmupRetryTimersByTarget = new Map();
		this._joinWarmupScheduledRetriesByTarget = new Map();
		this._repairSweepJoinWarmupGenerationByTarget = new Map();
	}

	getJoinWarmupGeneration(target: string) {
		let generation = this._joinWarmupGenerationByTarget.get(target);
		if (!generation) {
			generation = {};
			this._joinWarmupGenerationByTarget.set(target, generation);
		}
		return generation;
	}

	trackJoinWarmupTimer(target: string, timer: JoinWarmupRetryTimer) {
		let timers = this._joinWarmupRetryTimersByTarget.get(target);
		if (!timers) {
			timers = new Set();
			this._joinWarmupRetryTimersByTarget.set(target, timers);
		}
		timers.add(timer);
		this.deps.getRepairRetryTimers().add(timer.handle);
	}

	untrackJoinWarmupTimer(target: string, timer: JoinWarmupRetryTimer) {
		this.deps.getRepairRetryTimers().delete(timer.handle);
		const timers = this._joinWarmupRetryTimersByTarget.get(target);
		if (!timers) {
			return;
		}
		timers.delete(timer);
		if (timers.size === 0) {
			this._joinWarmupRetryTimersByTarget.delete(target);
		}
	}

	cancelJoinWarmupTimers(target: string) {
		const timers = this._joinWarmupRetryTimersByTarget.get(target);
		if (!timers) {
			return;
		}
		for (const timer of [...timers]) {
			clearTimeout(timer.handle);
			timer.resolve?.();
			this.untrackJoinWarmupTimer(target, timer);
		}
	}

	cancelJoinWarmupTarget(target: string) {
		this._joinWarmupGenerationByTarget.delete(target);
		this.deps.onTargetCancelled(target);
		this._repairSweepJoinWarmupGenerationByTarget.delete(target);
		this.cancelJoinWarmupTimers(target);
		this._joinWarmupScheduledRetriesByTarget.delete(target);
		const state = this._joinWarmupSendStateByTarget.get(target);
		if (!state) {
			return;
		}
		state.bypassKnownPeerHints = false;
		state.entries.clear();
		state.pending = false;
		if (!state.running) {
			this._joinWarmupSendStateByTarget.delete(target);
		}
	}

	cancelAllJoinWarmupTargets() {
		const targets = new Set([
			...this._joinWarmupGenerationByTarget.keys(),
			...this._joinWarmupRetryTimersByTarget.keys(),
			...this._joinWarmupScheduledRetriesByTarget.keys(),
			...this._joinWarmupSendStateByTarget.keys(),
		]);
		for (const target of targets) {
			this.cancelJoinWarmupTarget(target);
		}
	}

	async sleepJoinWarmupTracked(
		target: string,
		delayMs: number,
		repairLifecycleController: AbortController,
	) {
		if (delayMs <= 0) {
			return;
		}
		await new Promise<void>((resolve) => {
			let settled = false;
			let trackedTimer!: JoinWarmupRetryTimer;
			const settle = () => {
				if (settled) {
					return;
				}
				settled = true;
				if (
					repairLifecycleController ===
					this.deps.getCurrentLifecycleController()
				) {
					this.untrackJoinWarmupTimer(target, trackedTimer);
				}
				resolve();
			};
			const handle = setTimeout(settle, delayMs);
			handle.unref?.();
			trackedTimer = { handle, resolve: settle };
			this.trackJoinWarmupTimer(target, trackedTimer);
		});
	}

	scheduleJoinWarmupRetries(
		target: string,
		generation: object,
		delaysMs: Iterable<number>,
		entries: ReadonlyMap<string, E>,
		bypassKnownPeerHints: boolean,
		repairLifecycleController: AbortController = this.deps.getCurrentLifecycleController(),
	) {
		if (
			!this.deps.isLifecycleActive(repairLifecycleController) ||
			this._joinWarmupGenerationByTarget.get(target) !== generation
		) {
			return;
		}
		let scheduled = this._joinWarmupScheduledRetriesByTarget.get(target);
		if (scheduled?.generation !== generation) {
			this.cancelJoinWarmupTimers(target);
			this._joinWarmupScheduledRetriesByTarget.delete(target);
			scheduled = undefined;
		}
		if (!scheduled) {
			scheduled = {
				generation,
				slotsByDelay: new Map(),
			};
			this._joinWarmupScheduledRetriesByTarget.set(target, scheduled);
		}
		const delays = [...new Set(delaysMs)];
		const batch: JoinWarmupScheduledRetryBatch<E> = {
			bypassKnownPeerHints,
			entries: new Map(entries),
			remainingAttempts: delays.length,
		};
		const now = Date.now();
		for (const delayMs of delays) {
			let slot = scheduled.slotsByDelay.get(delayMs);
			if (!slot) {
				slot = { cohorts: [], head: 0 };
				scheduled.slotsByDelay.set(delayMs, slot);
			}
			const tail = slot.cohorts.at(-1);
			const dueAt = Math.max(tail?.dueAt ?? 0, now + delayMs);
			if (tail?.dueAt === dueAt) {
				tail.batches.push(batch);
			} else {
				slot.cohorts.push({
					batches: [batch],
					dueAt,
				});
			}
			this.armJoinWarmupRetrySlot(
				target,
				scheduled,
				delayMs,
				slot,
				repairLifecycleController,
			);
		}
	}

	armJoinWarmupRetrySlot(
		target: string,
		scheduled: JoinWarmupScheduledRetries<E>,
		delayMs: number,
		slot: JoinWarmupScheduledRetrySlot<E>,
		repairLifecycleController: AbortController,
	) {
		if (!this.deps.isLifecycleActive(repairLifecycleController)) {
			return;
		}
		const nextDueAt = slot.cohorts[slot.head]?.dueAt;
		if (nextDueAt == null) {
			return;
		}
		if (slot.timer && slot.timerDueAt === nextDueAt) {
			return;
		}
		if (slot.timer) {
			clearTimeout(slot.timer.handle);
			this.untrackJoinWarmupTimer(target, slot.timer);
		}
		let trackedTimer!: JoinWarmupRetryTimer;
		const handle = setTimeout(
			() => {
				if (!this.deps.isLifecycleActive(repairLifecycleController)) {
					return;
				}
				this.untrackJoinWarmupTimer(target, trackedTimer);
				if (slot.timer !== trackedTimer) {
					return;
				}
				slot.timer = undefined;
				slot.timerDueAt = undefined;
				const current = this._joinWarmupScheduledRetriesByTarget.get(target);
				if (
					current !== scheduled ||
					current.slotsByDelay.get(delayMs) !== slot
				) {
					return;
				}

				const dueEntries = new Map<string, E>();
				let bypassKnownPeerHints = false;
				const now = Date.now();
				while (
					slot.head < slot.cohorts.length &&
					slot.cohorts[slot.head].dueAt <= now
				) {
					const cohort = slot.cohorts[slot.head++];
					for (const batch of cohort.batches) {
						for (const [hash, entry] of batch.entries) {
							dueEntries.set(hash, entry);
						}
						bypassKnownPeerHints ||= batch.bypassKnownPeerHints;
						batch.remainingAttempts -= 1;
						if (batch.remainingAttempts === 0) {
							batch.entries.clear();
						}
					}
					cohort.batches.length = 0;
				}
				if (
					dueEntries.size > 0 &&
					!this.deps.isClosed() &&
					this._joinWarmupGenerationByTarget.get(target) ===
						scheduled.generation
				) {
					this.queueJoinWarmupSend(
						target,
						scheduled.generation,
						dueEntries,
						bypassKnownPeerHints,
						repairLifecycleController,
					);
				}
				if (slot.head === slot.cohorts.length) {
					current.slotsByDelay.delete(delayMs);
					if (current.slotsByDelay.size === 0) {
						this._joinWarmupScheduledRetriesByTarget.delete(target);
					}
					return;
				}
				if (slot.head >= 1_024 && slot.head * 2 >= slot.cohorts.length) {
					slot.cohorts = slot.cohorts.slice(slot.head);
					slot.head = 0;
				}
				this.armJoinWarmupRetrySlot(
					target,
					current,
					delayMs,
					slot,
					repairLifecycleController,
				);
			},
			Math.max(0, nextDueAt - Date.now()),
		);
		handle.unref?.();
		trackedTimer = { handle };
		slot.timer = trackedTimer;
		slot.timerDueAt = nextDueAt;
		this.trackJoinWarmupTimer(target, trackedTimer);
	}

	queueJoinWarmupSend(
		target: string,
		generation: object,
		entries: ReadonlyMap<string, E>,
		bypassKnownPeerHints: boolean,
		repairLifecycleController: AbortController = this.deps.getCurrentLifecycleController(),
	) {
		if (
			!this.deps.isLifecycleActive(repairLifecycleController) ||
			this._joinWarmupGenerationByTarget.get(target) !== generation
		) {
			return;
		}
		let state = this._joinWarmupSendStateByTarget.get(target);
		if (!state) {
			state = {
				bypassKnownPeerHints: false,
				entries: new Map(),
				generation,
				lastCompletedAt: Number.NEGATIVE_INFINITY,
				pending: false,
				running: false,
			};
			this._joinWarmupSendStateByTarget.set(target, state);
		} else if (state.generation !== generation) {
			state.bypassKnownPeerHints = false;
			state.entries.clear();
			state.pending = false;
		}
		for (const [hash, entry] of entries) {
			state.entries.set(hash, entry);
		}
		state.bypassKnownPeerHints ||= bypassKnownPeerHints;
		state.generation = generation;
		state.pending = true;
		if (state.running) {
			return;
		}
		void this.drainJoinWarmupSends(
			target,
			state,
			repairLifecycleController,
		).catch((error: any) => {
			if (this.deps.isLifecycleActive(repairLifecycleController)) {
				this.deps.logError(error);
			}
		});
	}

	async drainJoinWarmupSends(
		target: string,
		state: JoinWarmupSendState<E>,
		repairLifecycleController: AbortController,
	) {
		if (
			state.running ||
			!this.deps.isLifecycleActive(repairLifecycleController)
		) {
			return;
		}
		state.running = true;
		try {
			while (state.pending) {
				if (!this.deps.isLifecycleActive(repairLifecycleController)) {
					return;
				}
				state.pending = false;
				const generation = state.generation;
				const entries = new Map(state.entries);
				state.entries.clear();
				const bypassKnownPeerHints = state.bypassKnownPeerHints;
				state.bypassKnownPeerHints = false;
				const spacingMs = Math.max(
					0,
					state.lastCompletedAt + JOIN_WARMUP_SEND_SPACING_MS - Date.now(),
				);
				await this.sleepJoinWarmupTracked(
					target,
					spacingMs,
					repairLifecycleController,
				);
				if (
					!this.deps.isLifecycleActive(repairLifecycleController) ||
					state.generation !== generation ||
					this._joinWarmupGenerationByTarget.get(target) !== generation
				) {
					continue;
				}
				if (entries.size === 0) {
					continue;
				}
				this.deps.bumpSimpleFallbackPasses();
				try {
					await this.deps.sendEntriesSimple(target, entries, {
						bypassKnownPeers: bypassKnownPeerHints,
						bypassRecentKnownPeers: bypassKnownPeerHints,
						isStillCurrent: () =>
							this.deps.isLifecycleActive(repairLifecycleController),
						signal: repairLifecycleController.signal,
					});
				} catch (error: any) {
					if (this.deps.isLifecycleActive(repairLifecycleController)) {
						this.deps.logError(error);
					}
				} finally {
					if (this.deps.isLifecycleActive(repairLifecycleController)) {
						state.lastCompletedAt = Date.now();
					}
				}
			}
		} finally {
			state.running = false;
			if (this._joinWarmupSendStateByTarget.get(target) === state) {
				const currentRepairLifecycleController =
					this.deps.getCurrentLifecycleController();
				if (
					state.pending &&
					this.deps.isLifecycleActive(currentRepairLifecycleController)
				) {
					void this.drainJoinWarmupSends(
						target,
						state,
						currentRepairLifecycleController,
					).catch((error: any) => this.deps.logError(error));
				} else if (!this._joinWarmupGenerationByTarget.has(target)) {
					this._joinWarmupSendStateByTarget.delete(target);
				}
			}
		}
	}
}
