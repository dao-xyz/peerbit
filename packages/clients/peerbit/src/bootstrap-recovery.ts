import type { Multiaddr } from "@multiformats/multiaddr";

/**
 * Automatic bootstrap recovery is opt-in so creating a Peerbit client keeps
 * its existing no-network-side-effect behavior unless an application asks for
 * recovery explicitly.
 */
export type BootstrapRecoveryOptions = {
	/** Set to false to leave recovery disabled. Defaults to true for an options object. */
	enabled?: boolean;
	/**
	 * Fixed bootstrap targets. When omitted, every recovery attempt resolves the
	 * current public bootstrap list, including its canonical fallback source.
	 */
	addresses?: Array<string | Multiaddr>;
	/**
	 * Delay after the first failed attempt. Defaults to 1 second; maximum is
	 * 2,147,483,647 ms.
	 */
	initialDelayMs?: number;
	/**
	 * Maximum retry delay, including jitter. Defaults to 60 seconds; maximum is
	 * 2,147,483,647 ms.
	 */
	maxDelayMs?: number;
	/** Exponential multiplier applied after each failed attempt. Defaults to 2. */
	backoffFactor?: number;
	/** Symmetric jitter ratio in the inclusive range 0..1. Defaults to 0.2. */
	jitter?: number;
	/**
	 * Minimum time between attempts and reconnects. Defaults to 1 second;
	 * maximum is 2,147,483,647 ms.
	 */
	cooldownMs?: number;
};

type NormalizedBootstrapRecoveryOptions = {
	initialDelayMs: number;
	maxDelayMs: number;
	backoffFactor: number;
	jitter: number;
	cooldownMs: number;
};

export type BootstrapRecoveryEventTarget = {
	addEventListener(type: string, listener: EventListener): void;
	removeEventListener(type: string, listener: EventListener): void;
};

export type BootstrapRecoveryRuntime = {
	bootstrap(signal: AbortSignal): Promise<unknown>;
	connectionEvents: BootstrapRecoveryEventTarget;
	onlineEvents?: BootstrapRecoveryEventTarget;
	isConnected(): boolean;
	isOnline?: () => boolean;
	now?: () => number;
	random?: () => number;
	onError?: (error: unknown) => void;
};

const DEFAULT_OPTIONS: NormalizedBootstrapRecoveryOptions = {
	initialDelayMs: 1_000,
	maxDelayMs: 60_000,
	backoffFactor: 2,
	jitter: 0.2,
	cooldownMs: 1_000,
};

const MAX_TIMER_DELAY_MS = 2_147_483_647;

const finiteNumber = (name: string, value: number, minimum: number): number => {
	if (!Number.isFinite(value) || value < minimum) {
		throw new Error(`${name} must be a finite number >= ${minimum}`);
	}
	return value;
};

const timerDelay = (name: string, value: number, minimum: number): number => {
	const delay = finiteNumber(name, value, minimum);
	if (delay > MAX_TIMER_DELAY_MS) {
		throw new Error(`${name} must be <= ${MAX_TIMER_DELAY_MS}`);
	}
	return delay;
};

const normalizeOptions = (
	options: BootstrapRecoveryOptions,
): NormalizedBootstrapRecoveryOptions => {
	const initialDelayMs = timerDelay(
		"bootstrapRecovery.initialDelayMs",
		options.initialDelayMs ?? DEFAULT_OPTIONS.initialDelayMs,
		1,
	);
	const maxDelayMs = timerDelay(
		"bootstrapRecovery.maxDelayMs",
		options.maxDelayMs ?? DEFAULT_OPTIONS.maxDelayMs,
		initialDelayMs,
	);
	const backoffFactor = finiteNumber(
		"bootstrapRecovery.backoffFactor",
		options.backoffFactor ?? DEFAULT_OPTIONS.backoffFactor,
		1,
	);
	const jitter = finiteNumber(
		"bootstrapRecovery.jitter",
		options.jitter ?? DEFAULT_OPTIONS.jitter,
		0,
	);
	if (jitter > 1) {
		throw new Error("bootstrapRecovery.jitter must be <= 1");
	}
	const cooldownMs = timerDelay(
		"bootstrapRecovery.cooldownMs",
		options.cooldownMs ?? DEFAULT_OPTIONS.cooldownMs,
		0,
	);
	return {
		initialDelayMs,
		maxDelayMs,
		backoffFactor,
		jitter,
		cooldownMs,
	};
};

/** Validate policy configuration before an owning client acquires resources. */
export const validateBootstrapRecoveryOptions = (
	options: BootstrapRecoveryOptions,
): void => {
	if (options.addresses?.length === 0) {
		throw new Error("bootstrapRecovery.addresses must not be empty");
	}
	normalizeOptions(options);
};

/** Internal lifecycle controller, exported from its source module for tests. */
export class BootstrapRecoveryController {
	private readonly options: NormalizedBootstrapRecoveryOptions;
	private readonly now: () => number;
	private readonly random: () => number;
	private running = false;
	private timer?: ReturnType<typeof setTimeout>;
	private timerDueAt?: number;
	private inFlight?: Promise<void>;
	private attemptAbort?: AbortController;
	private consecutiveFailures = 0;
	private lastAttemptAt = Number.NEGATIVE_INFINITY;
	private retryNotBefore = Number.NEGATIVE_INFINITY;

	private readonly onConnectionOpen: EventListener = () => {
		// A bootstrap attempt emits this event from inside its own successful dial.
		// Let that attempt finish all Peerbit bootstrap side effects before treating
		// the recovery as complete.
		if (this.inFlight) return;
		this.markConnected();
	};

	private readonly onConnectionClose: EventListener = () => {
		this.requestRecovery(false);
	};

	private readonly onOnline: EventListener = () => {
		this.requestRecovery(true);
	};

	constructor(
		private readonly runtime: BootstrapRecoveryRuntime,
		options: BootstrapRecoveryOptions = {},
	) {
		this.options = normalizeOptions(options);
		this.now = runtime.now ?? Date.now;
		this.random = runtime.random ?? Math.random;
	}

	get started(): boolean {
		return this.running;
	}

	/** Whether this controller currently owns a pending recovery timer. */
	get hasScheduledRecovery(): boolean {
		return this.timer !== undefined;
	}

	start(): void {
		if (this.running) return;
		this.running = true;
		this.runtime.connectionEvents.addEventListener(
			"connection:open",
			this.onConnectionOpen,
		);
		this.runtime.connectionEvents.addEventListener(
			"connection:close",
			this.onConnectionClose,
		);
		this.runtime.onlineEvents?.addEventListener("online", this.onOnline);

		if (this.runtime.isConnected()) {
			this.markConnected();
		} else {
			this.schedule(0, false);
		}
	}

	stop(): Promise<void> {
		if (this.running) {
			this.running = false;
			this.runtime.connectionEvents.removeEventListener(
				"connection:open",
				this.onConnectionOpen,
			);
			this.runtime.connectionEvents.removeEventListener(
				"connection:close",
				this.onConnectionClose,
			);
			this.runtime.onlineEvents?.removeEventListener("online", this.onOnline);
			this.clearTimer();
			this.attemptAbort?.abort(new Error("Bootstrap recovery stopped"));
			this.attemptAbort = undefined;
		}
		return this.inFlight ?? Promise.resolve();
	}

	private markConnected(): void {
		if (!this.running) return;
		this.consecutiveFailures = 0;
		this.retryNotBefore = Number.NEGATIVE_INFINITY;
		this.lastAttemptAt = this.now();
		this.clearTimer();
	}

	private requestRecovery(urgent: boolean): void {
		if (!this.running || this.runtime.isConnected() || this.inFlight) return;
		const now = this.now();
		const cooldownRemaining = Math.max(
			0,
			this.lastAttemptAt + this.options.cooldownMs - now,
		);
		const retryRemaining = Math.max(0, this.retryNotBefore - now);
		this.schedule(Math.max(cooldownRemaining, retryRemaining), urgent);
	}

	private clearTimer(): void {
		if (this.timer) {
			clearTimeout(this.timer);
		}
		this.timer = undefined;
		this.timerDueAt = undefined;
	}

	private schedule(delayMs: number, replaceIfEarlier: boolean): void {
		if (!this.running) return;
		const delay = Math.max(0, delayMs);
		const dueAt = this.now() + delay;
		if (this.timer) {
			if (!replaceIfEarlier || (this.timerDueAt ?? dueAt) <= dueAt) {
				return;
			}
			this.clearTimer();
		}
		this.timerDueAt = dueAt;
		this.timer = setTimeout(() => {
			this.timer = undefined;
			this.timerDueAt = undefined;
			this.runAttempt();
		}, delay);
		(
			this.timer as ReturnType<typeof setTimeout> & { unref?: () => void }
		).unref?.();
	}

	private runAttempt(): void {
		if (!this.running || this.runtime.isConnected()) {
			if (this.runtime.isConnected()) this.markConnected();
			return;
		}
		if (this.runtime.isOnline?.() === false) {
			// Browser online events bring this forward immediately; the bounded timer
			// remains as a safety net for missed or unreliable environment signals.
			this.schedule(this.options.maxDelayMs, false);
			return;
		}
		const cooldownRemaining = Math.max(
			0,
			this.lastAttemptAt + this.options.cooldownMs - this.now(),
		);
		if (cooldownRemaining > 0) {
			this.schedule(cooldownRemaining, false);
			return;
		}
		if (this.inFlight) return;

		this.lastAttemptAt = this.now();
		const controller = new AbortController();
		this.attemptAbort = controller;
		const attempt = Promise.resolve().then(async () => {
			await this.runtime.bootstrap(controller.signal);
		});
		const flight = attempt
			.catch((error) => {
				if (this.running && !controller.signal.aborted) {
					this.runtime.onError?.(error);
				}
			})
			.finally(() => {
				if (this.inFlight !== flight) return;
				this.inFlight = undefined;
				if (this.attemptAbort === controller) {
					this.attemptAbort = undefined;
				}
				if (!this.running) return;
				if (this.runtime.isConnected()) {
					this.markConnected();
					return;
				}
				// A bootstrap call that returns after its connection has already closed is
				// still a recovery failure and must remain on the bounded retry path.
				this.scheduleRetry();
			});
		this.inFlight = flight;
	}

	private scheduleRetry(): void {
		const exponent = Math.min(this.consecutiveFailures, 52);
		const baseDelay = Math.min(
			this.options.maxDelayMs,
			this.options.initialDelayMs *
				Math.pow(this.options.backoffFactor, exponent),
		);
		this.consecutiveFailures += 1;
		const random = Math.max(0, Math.min(1, this.random()));
		const jitterMultiplier = 1 + (random * 2 - 1) * this.options.jitter;
		const retryDelay = Math.max(
			1,
			Math.min(
				this.options.maxDelayMs,
				Math.round(baseDelay * jitterMultiplier),
			),
		);
		const now = this.now();
		const cooldownRemaining = Math.max(
			0,
			this.lastAttemptAt + this.options.cooldownMs - now,
		);
		const delay = Math.max(retryDelay, cooldownRemaining);
		this.retryNotBefore = now + delay;
		this.schedule(delay, false);
	}
}
