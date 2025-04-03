export const debounceFixedInterval = <
	T extends (...args: any[]) => any | Promise<any>,
>(
	fn: T,
	delay: number | (() => number),
	options?: { onError?: (error: Error) => void; leading?: boolean },
): { call: (...args: Parameters<T>) => Promise<void>; close: () => void } => {
	// A debounce function that waits for the delay after the async function finishes
	// before invoking the function again, and returns a promise that resolves when the invocation is done.
	let delayFn: () => number = typeof delay === "number" ? () => delay : delay;
	const onError =
		options?.onError ||
		((error: Error) => {
			throw error;
		});
	// When leading is true, the first call is immediate. Otherwise, it's deferred.
	const leading = options?.leading !== undefined ? options.leading : true;
	let timeout: NodeJS.Timeout | null = null;
	let lastArgs: any[] | null = null;
	let lastThis: any;
	let pendingCall = false;
	let isRunning = false;
	// Array of resolvers for the promises returned by each debounced call.
	let waitingResolvers: Array<() => void> = [];
	// Track when the last invocation finished.
	let lastInvokeTime: number | null = null;

	const invoke = async () => {
		timeout = null;
		if (!lastArgs) {
			return;
		}
		const args = lastArgs;
		lastArgs = null;
		pendingCall = false;
		isRunning = true;
		try {
			await Promise.resolve(fn.apply(lastThis, args));
		} catch (error) {
			onError(error as Error);
		} finally {
			isRunning = false;
			lastInvokeTime = Date.now();
			// Resolve all waiting promises.
			waitingResolvers.forEach((resolve) => resolve());
			waitingResolvers = [];
			// If calls came in during the invocation, schedule the next run.
			if (pendingCall) {
				const timeSinceInvoke = Date.now() - (lastInvokeTime || 0);
				const delayRemaining = Math.max(delayFn() - timeSinceInvoke, 0);
				timeout = setTimeout(invoke, delayRemaining);
			}
		}
	};

	const debounced = (...args: Parameters<T>): Promise<void> => {
		lastArgs = args;
		lastThis = this;
		pendingCall = true;
		// Create a new promise that will resolve when the associated invocation completes.
		const p = new Promise<void>((resolve) => {
			waitingResolvers.push(resolve);
		});

		const now = Date.now();
		if (!isRunning && !timeout) {
			if (leading) {
				// If leading is enabled, trigger immediately if it's the very first call
				// or if enough time has passed since the last invocation.
				if (lastInvokeTime === null || now - lastInvokeTime >= delayFn()) {
					invoke();
				} else {
					const delayRemaining = delayFn() - (now - lastInvokeTime);
					timeout = setTimeout(invoke, delayRemaining);
				}
			} else {
				// If not leading, always schedule a trailing invocation.
				timeout = setTimeout(invoke, delayFn());
			}
		}
		return p;
	};

	return {
		call: debounced,
		close: () => {
			isRunning = false;
			if (timeout !== null) {
				clearTimeout(timeout);
			}
		},
	};
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

	const invoke = async () => {
		const toSend = accumulator.value;
		accumulator = create();
		await fn(toSend);
	};

	const { call: debounced, close } = debounceFixedInterval(
		invoke,
		delay,
		options,
	);

	return {
		add: (value: T): Promise<void> => {
			accumulator.add(value);
			// Return a promise that resolves when the debounced (accumulated) call is executed.
			return debounced();
		},
		delete: (key: K) => {
			accumulator.delete(key);
		},
		size: () => accumulator.size(),
		invoke,
		close: () => close(),
		has: (key: K) => accumulator.has(key),
	};
};
