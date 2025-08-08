export const debounceFixedInterval = <
	T extends (...args: any[]) => any | Promise<any>,
>(
	fn: T,
	delay: number | (() => number),
	options?: { onError?: (error: Error) => void; leading?: boolean },
): {
	call: (...args: Parameters<T>) => Promise<void>;
	close: () => void;
	flush: () => Promise<void>;
} => {
	const delayFn: () => number = typeof delay === "number" ? () => delay : delay;
	const onError =
		options?.onError ??
		((e: Error) => {
			throw e;
		});
	const leading = options?.leading ?? true;

	let timeout: ReturnType<typeof setTimeout> | null = null;
	let lastArgs: any[] | null = null;
	let lastThis: any;
	let pendingCall = false; // there is queued work for the *next* run
	let isRunning = false; // fn is executing right now
	let waitingResolvers: Array<() => void> = []; // resolve when *a* run completes
	let lastInvokeTime: number | null = null;
	let forceNextImmediate = false;

	// Completed run counter + precise run waiters
	let completedRuns = 0;
	type RunWaiter = { target: number; resolve: () => void };
	let runWaiters: RunWaiter[] = [];

	const resolveRunWaiters = () => {
		if (runWaiters.length === 0) return;
		const remaining: RunWaiter[] = [];
		for (const w of runWaiters) {
			if (completedRuns >= w.target) {
				w.resolve();
			} else {
				remaining.push(w);
			}
		}
		runWaiters = remaining;
	};

	const waitForRun = (target: number) =>
		new Promise<void>((resolve) => {
			if (completedRuns >= target) return resolve();
			runWaiters.push({ target, resolve });
		});

	const invoke = async () => {
		timeout = null;
		if (!lastArgs) return; // nothing to invoke

		const args = lastArgs;
		const ctx = lastThis;
		lastArgs = null; // consume current args
		pendingCall = false; // this run is for those args
		isRunning = true;

		try {
			await Promise.resolve(fn.apply(ctx, args));
		} catch (err) {
			onError(err as Error);
		} finally {
			isRunning = false;
			lastInvokeTime = Date.now();

			// Resolve all call() promises queued for this completed run
			const resolvers = waitingResolvers;
			waitingResolvers = [];
			for (const r of resolvers) r();

			// Mark completion and resolve any run-target waiters that are due
			completedRuns++;
			resolveRunWaiters();

			// If new calls arrived during this run, schedule the next one
			if (pendingCall) {
				if (forceNextImmediate) {
					forceNextImmediate = false;
					timeout = setTimeout(invoke, 0);
				} else {
					const elapsed = Date.now() - (lastInvokeTime ?? 0);
					const remaining = Math.max(delayFn() - elapsed, 0);
					timeout = setTimeout(invoke, remaining);
				}
			}
		}
	};

	// Use a normal function to preserve `this` from the call site
	function debounced(this: any, ...args: Parameters<T>): Promise<void> {
		lastArgs = args;
		lastThis = this;
		pendingCall = true;

		// Resolve after the next completed run
		const p = new Promise<void>((resolve) => {
			waitingResolvers.push(resolve);
		});

		const now = Date.now();
		if (!isRunning && !timeout) {
			if (leading) {
				if (lastInvokeTime === null || now - lastInvokeTime >= delayFn()) {
					invoke();
				} else {
					const remaining = delayFn() - (now - lastInvokeTime);
					timeout = setTimeout(invoke, remaining);
				}
			} else {
				timeout = setTimeout(invoke, delayFn());
			}
		}
		return p;
	}

	const flush = (): Promise<void> => {
		if (isRunning) {
			// If there are pending args, ensure a trailing run and make it immediate
			const hadPendingArgs = !!lastArgs;
			if (hadPendingArgs) {
				pendingCall = true;
				forceNextImmediate = true;
			}
			// Wait for the current run (+1) and, if needed, the immediate trailing run (+1)
			const target = completedRuns + 1 + (hadPendingArgs ? 1 : 0);
			return waitForRun(target);
		}

		// Not running
		if (timeout) {
			clearTimeout(timeout);
			timeout = null;
		}

		if (lastArgs) {
			const target = completedRuns + 1; // we'll trigger a run now
			invoke();
			return waitForRun(target);
		} else {
			// nothing to flush
			return Promise.resolve();
		}
	};

	const close = () => {
		if (timeout !== null) {
			clearTimeout(timeout);
			timeout = null;
		}
		isRunning = false;
		forceNextImmediate = false;
		// no auto-resolving of pending promises on close()
	};

	return { call: debounced, close, flush };
};

export const debounceAccumulator = <K, T, V>(
	fn: (args: V) => any,
	create: () => {
		delete: (key: K) => void;
		add: (value: T) => void;
		size: () => number;
		value: V;
		has: (key: K) => boolean;
	},
	delay: number | (() => number),
	options?: { leading?: boolean },
) => {
	let accumulator = create();

	const innerInvoke = async () => {
		const toSend = accumulator.value;
		accumulator = create();
		await fn(toSend);
	};

	const deb = debounceFixedInterval(innerInvoke, delay, options);

	return {
		add: (value: T): Promise<void> => {
			accumulator.add(value);
			// resolves when the batch (which includes this value) runs
			return deb.call();
		},
		delete: (key: K) => {
			accumulator.delete(key);
		},
		size: () => accumulator.size(),
		has: (key: K) => accumulator.has(key),

		// Run immediately, and **cancel** any pending scheduled run to avoid a trailing empty run.
		invoke: async (): Promise<void> => {
			deb.close(); // cancel any pending timeout
			await innerInvoke();
		},

		// Cancel pending schedule AND reset accumulator so size() === 0 afterward.
		close: (): void => {
			deb.close();
			accumulator = create();
		},

		// If you exposed flush() before, keep passing it through:
		flush: (): Promise<void> => deb.flush?.() ?? Promise.resolve(),
	};
};
