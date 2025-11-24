export const debounceFixedInterval = <
	T extends (...args: any[]) => any | Promise<any>,
>(
	fn: T,
	delay: number | (() => number),
	options?: { onError?(error: Error): void; leading?: boolean },
): {
	call(...args: Parameters<T>): Promise<void>;
	close(): void;
	flush(): Promise<void>;
} => {
	const delayFn: () => number = typeof delay === "number" ? () => delay : delay;
	const onError =
		options?.onError ??
		((e: Error): void => {
			throw e;
		});
	const leading = options?.leading ?? true;

	let timeout: ReturnType<typeof setTimeout> | null = null;
	let lastArgs: any[] | null = null;
	let lastThis: any;
	let pendingCall = false;
	let isRunning = false;
	let waitingResolvers: Array<() => void> = [];
	let lastInvokeTime: number | null = null;
	let forceNextImmediate = false;

	let completedRuns = 0;
	type RunWaiter = { target: number; resolve(): void };
	let runWaiters: RunWaiter[] = [];

	const resolveRunWaiters = (): void => {
		if (runWaiters.length === 0) {
			return;
		}
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

	const waitForRun = (target: number): Promise<void> =>
		new Promise<void>((resolve) => {
			if (completedRuns >= target) {
				resolve();
				return;
			}
			runWaiters.push({ target, resolve });
		});

	const invoke = async (): Promise<void> => {
		timeout = null;
		if (!lastArgs) {
			return;
		}

		const args = lastArgs;
		const ctx = lastThis;
		lastArgs = null;
		pendingCall = false;
		isRunning = true;

		try {
			await Promise.resolve(fn.apply(ctx, args));
		} catch (err) {
			onError(err as Error);
		} finally {
			isRunning = false;
			lastInvokeTime = Date.now();

			const resolvers = waitingResolvers;
			waitingResolvers = [];
			for (const r of resolvers) {
				r();
			}

			completedRuns++;
			resolveRunWaiters();

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

	function debounced(this: any, ...args: Parameters<T>): Promise<void> {
		lastArgs = args;
		lastThis = this;
		pendingCall = true;

		const p = new Promise<void>((resolve) => {
			waitingResolvers.push(resolve);
		});

		const now = Date.now();
		if (!isRunning && !timeout) {
			if (leading) {
				if (lastInvokeTime === null || now - lastInvokeTime >= delayFn()) {
					void invoke();
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
			const hadPendingArgs = Boolean(lastArgs);
			if (hadPendingArgs) {
				pendingCall = true;
				forceNextImmediate = true;
			}
			const target = completedRuns + 1 + (hadPendingArgs ? 1 : 0);
			return waitForRun(target);
		}

		if (timeout) {
			clearTimeout(timeout);
			timeout = null;
		}

		if (lastArgs) {
			const target = completedRuns + 1;
			void invoke();
			return waitForRun(target);
		}

		return Promise.resolve();
	};

	const close = (): void => {
		if (timeout !== null) {
			clearTimeout(timeout);
			timeout = null;
		}
		isRunning = false;
		forceNextImmediate = false;
	};

	return { call: debounced, close, flush };
};

export const debounceAccumulator = <K, T, V>(
	fn: (args: V) => any,
	create: () => {
		delete(key: K): void;
		add(value: T): void;
		size(): number;
		value: V;
		has(key: K): boolean;
	},
	delay: number | (() => number),
	options?: { leading?: boolean },
): {
	add(value: T): Promise<void>;
	delete(key: K): void;
	size(): number;
	has(key: K): boolean;
	invoke(): Promise<void>;
	close(): void;
	flush(): Promise<void>;
} => {
	let accumulator = create();

	const innerInvoke = async (): Promise<void> => {
		const toSend = accumulator.value;
		accumulator = create();
		await fn(toSend);
	};

	const deb = debounceFixedInterval(innerInvoke, delay, options);

	return {
		add: (value: T): Promise<void> => {
			accumulator.add(value);
			return deb.call();
		},
		delete: (key: K): void => {
			accumulator.delete(key);
		},
		size: (): number => accumulator.size(),
		has: (key: K): boolean => accumulator.has(key),
		invoke: async (): Promise<void> => {
			deb.close();
			await innerInvoke();
		},
		close: (): void => {
			deb.close();
			accumulator = create();
		},
		flush: (): Promise<void> => deb.flush?.() ?? Promise.resolve(),
	};
};
