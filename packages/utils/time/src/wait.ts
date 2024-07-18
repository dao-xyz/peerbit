export class TimeoutError extends Error {}

export class AbortError extends Error {}
export const delay = async (ms: number, options?: { signal?: AbortSignal }) => {
	return new Promise<void>((resolve, reject) => {
		function handleAbort() {
			clearTimeout(timer);
			reject(new AbortError());
		}
		options?.signal?.addEventListener("abort", handleAbort);
		const timer = setTimeout(() => {
			options?.signal?.removeEventListener("abort", handleAbort);
			resolve();
		}, ms);
	});
};

const createTimeoutError = (options: { timeoutMessage?: string }) =>
	new TimeoutError(
		options?.timeoutMessage
			? "Timed out: " + options?.timeoutMessage
			: "Timed out",
	);

export const waitFor = async <T>(
	fn: () => T | Promise<T>,
	options: {
		timeout?: number;
		signal?: AbortSignal;
		delayInterval?: number;
		timeoutMessage?: string;
	} = { timeout: 10 * 1000, delayInterval: 100 },
): Promise<T | undefined> => {
	const delayInterval = options.delayInterval || 100;
	const timeout = options.timeout || 10 * 1000;
	const startTime = Number(new Date());
	let stop = false;

	const handleAbort = () => {
		stop = true;
		options.signal?.removeEventListener("abort", handleAbort);
	};

	options.signal?.addEventListener("abort", handleAbort);

	// eslint-disable-next-line no-unmodified-loop-condition
	while (!stop && Number(new Date()) - startTime < timeout) {
		const result = await fn();
		if (result) {
			options.signal?.removeEventListener("abort", handleAbort);
			return result;
		}

		await delay(delayInterval, options);
	}
	throw createTimeoutError(options);
};

export const waitForResolved = async <T>(
	fn: () => T | Promise<T>,
	options: {
		timeout?: number;
		signal?: AbortSignal;
		delayInterval?: number;
		timeoutMessage?: string;
	} = { timeout: 10 * 1000, delayInterval: 50 },
): Promise<T> => {
	const delayInterval = options.delayInterval || 50;
	const timeout = options.timeout || 10 * 1000;

	const startTime = Number(new Date());
	let stop = false;
	let lastError: Error | undefined;

	const handleAbort = () => {
		stop = true;
		options.signal?.removeEventListener("abort", handleAbort);
	};

	options.signal?.addEventListener("abort", handleAbort);

	// eslint-disable-next-line no-unmodified-loop-condition
	while (!stop && Number(new Date()) - startTime < timeout) {
		try {
			const result = await fn();
			options.signal?.removeEventListener("abort", handleAbort);
			return result;
		} catch (error: any) {
			if (!(error instanceof AbortError)) {
				lastError = error;
			} else {
				throw error;
			}
		}
		await delay(delayInterval, options);
	}

	throw lastError || createTimeoutError(options);
};
