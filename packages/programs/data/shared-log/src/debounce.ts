/* export const debounceFixedInterval = <
	T extends (...args: any[]) => any | Promise<any>,
>(
	fn: T,
	delay: number,
	options?: { debug?: boolean, onError?: (error: Error) => void },
) => {
	// a debounce function that will always wait for the delay to pass before invoking the function
	// though if delay time has passed it will call the function immediately instead of resetting the timer

	let onError = options?.onError || ((error: Error) => console.error(error));
	let timeout: NodeJS.Timeout | null = null;
	let lastArgs: any[] = [];
	let lastThis: any;
	let invokePromise = Promise.resolve();
	const invoke = async () => {
		const fnCall = fn.apply(lastThis, lastArgs);
		invokePromise = Promise.resolve(fnCall ?? {});
		await invokePromise.then((res) => {
			
			timeout = null;
			return res;
		}).catch(onError);
	};

	const debounced = (...args: Parameters<T>) => {
		lastArgs = args;
		lastThis = this;
		if (timeout) {
			return;
		}
		invokePromise.then(() => {
			timeout && clearTimeout(timeout);
			timeout = setTimeout(invoke, delay);
			if (options?.debug) {
				console.log("debounceFixedInterval: timeout set", timeout);
			}
		});
	};

	return debounced as T;
};
 */
export const debounceFixedInterval = <
	T extends (...args: any[]) => any | Promise<any>,
>(
	fn: T,
	delay: number | (() => number),
	options?: { onError?: (error: Error) => void },
) => {
	// A debounce function that waits for the delay after the async function finishes
	// before invoking the function again
	let delayFn: () => number = typeof delay === "number" ? () => delay : delay;
	const onError =
		options?.onError ||
		((error: Error) => {
			throw error;
		});
	let timeout: NodeJS.Timeout | null = null;
	let lastArgs: any[] | null = null;
	let lastThis: any;
	let invokePromise: Promise<any> = Promise.resolve();
	let pendingCall = false;
	let isRunning = false;

	const invoke = async () => {
		if (!lastArgs) {
			// No pending calls to process
			timeout = null;
			return;
		}

		const args = lastArgs;
		lastArgs = null; // Reset arguments
		pendingCall = false; // Reset pending call flag
		isRunning = true;
		try {
			const fnCall = fn.apply(lastThis, args);
			invokePromise = Promise.resolve(fnCall ?? {});

			await invokePromise;
		} catch (error) {
			onError(error as Error);
		} finally {
			isRunning = false;
			timeout = null;

			// If there are pending calls, schedule the next invocation
			if (pendingCall) {
				timeout = setTimeout(invoke, delayFn());
			}
		}
	};

	const debounced = (...args: Parameters<T>) => {
		lastArgs = args;
		lastThis = this;
		pendingCall = true;

		if (isRunning || timeout) {
			// Function is currently running or timeout is set, do nothing
			return;
		}

		// No function running and no timeout set, so schedule the invocation
		timeout = setTimeout(invoke, delayFn());
	};

	return debounced as T;
};

export const debounceAcculmulator = <K, T, V>(
	fn: (args: V) => any,
	create: () => {
		delete: (string: K) => void;
		add: (value: T) => void;
		size: () => number;
		value: V;
	},
	delay: number | (() => number),
) => {
	let accumulator = create();

	const invoke = async () => {
		let toSend = accumulator.value;
		accumulator = create();
		await fn(toSend);
	};

	const debounced = debounceFixedInterval(invoke, delay);

	return {
		add: (value: T) => {
			accumulator.add(value);
			debounced();
		},
		delete: (key: K) => {
			accumulator.delete(key);
		},
		size: () => accumulator.size,
		invoke,
	};
};

export const debouncedAccumulatorMap = <T>(
	fn: (args: Map<string, T>) => any,
	delay: number,
) => {
	return debounceAcculmulator<
		string,
		{ key: string; value: T },
		Map<string, T>
	>(
		fn,
		() => {
			const map = new Map();
			return {
				add: (props: { key: string; value: T }) =>
					map.set(props.key, props.value),
				delete: (key: string) => map.delete(key),
				size: () => map.size,
				value: map,
				clear: () => map.clear(),
			};
		},
		delay,
	);
};

export type DebouncedAccumulatorMap<T> = ReturnType<
	typeof debouncedAccumulatorMap<T>
>;
