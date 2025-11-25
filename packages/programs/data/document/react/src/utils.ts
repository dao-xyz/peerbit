export function debounceLeadingTrailing<
	T extends (this: any, ...args: any[]) => void,
>(
	func: T,
	delay: number,
): ((this: ThisParameterType<T>, ...args: Parameters<T>) => void) & {
	cancel: () => void;
} {
	let timeoutId: ReturnType<typeof setTimeout> | null = null;
	let lastArgs: Parameters<T> | null = null;
	let lastThis: any;
	let pendingTrailing = false;

	const debounced = function (
		this: ThisParameterType<T>,
		...args: Parameters<T>
	) {
		if (!timeoutId) {
			// Leading call: no timer means this is the first call in this period.
			func.apply(this, args);
		} else {
			// Subsequent calls during the delay mark that a trailing call is needed.
			pendingTrailing = true;
		}
		// Always update with the most recent context and arguments.
		lastArgs = args;
		lastThis = this;

		// Reset the timer.
		if (timeoutId) {
			clearTimeout(timeoutId);
		}
		timeoutId = setTimeout(() => {
			timeoutId = null;
			// If there were any calls during the delay, call the function on the trailing edge.
			if (pendingTrailing && lastArgs) {
				func.apply(lastThis, lastArgs);
			}
			// Reset the trailing flag after the trailing call.
			pendingTrailing = false;
		}, delay);
	} as ((this: ThisParameterType<T>, ...args: Parameters<T>) => void) & {
		cancel: () => void;
	};

	debounced.cancel = () => {
		if (timeoutId) {
			clearTimeout(timeoutId);
			timeoutId = null;
		}
		pendingTrailing = false;
	};

	return debounced;
}
