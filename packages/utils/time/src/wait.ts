export class TimeoutError extends Error {}

export class AbortError extends Error {}

export const delay = async (
	ms: number,
	options?: { signal?: AbortSignal },
): Promise<void> => {
	return new Promise<void>((resolve, reject) => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		const handleAbort = (): void => {
			options?.signal?.removeEventListener("abort", handleAbort);
			if (timer) clearTimeout(timer);
			reject(new AbortError());
		};

		if (options?.signal) {
			if (options.signal.aborted) {
				handleAbort();
				return;
			}
			options.signal.addEventListener("abort", handleAbort, { once: true });
		}

		timer = setTimeout(() => {
			options?.signal?.removeEventListener("abort", handleAbort);
			resolve();
		}, ms);
	});
};

const createTimeoutError = (options: {
	timeoutMessage?: string;
}): TimeoutError =>
	new TimeoutError(
		options?.timeoutMessage
			? "Timed out: " + options.timeoutMessage
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
	const delayInterval = options.delayInterval ?? 100;
	const timeout = options.timeout ?? 10 * 1000;
	const startTime = Date.now();
	let stop = false;

	let aborted = false;
	const handleAbort = (): void => {
		stop = true;
		aborted = true;
		options.signal?.removeEventListener("abort", handleAbort);
	};

	options.signal?.addEventListener("abort", handleAbort);
	if (options?.signal?.aborted) {
		handleAbort();
	}

	// eslint-disable-next-line no-unmodified-loop-condition
	while (!stop && Date.now() - startTime < timeout) {
		const result = await fn();
		if (result) {
			options.signal?.removeEventListener("abort", handleAbort);
			return result;
		}

		await delay(delayInterval, options);
	}
	if (aborted) {
		throw new AbortError();
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
	const delayInterval = options.delayInterval ?? 50;
	const timeout = options.timeout ?? 10 * 1000;

	const startTime = Date.now();
	let stop = false;
	let lastError: Error | undefined;

	let aborted = false;
	const handleAbort = (): void => {
		stop = true;
		aborted = true;
		options.signal?.removeEventListener("abort", handleAbort);
	};

	options.signal?.addEventListener("abort", handleAbort);
	if (options?.signal?.aborted) {
		handleAbort();
	}

	// eslint-disable-next-line no-unmodified-loop-condition
	while (!stop && Date.now() - startTime < timeout) {
		try {
			const result = await fn();
			options.signal?.removeEventListener("abort", handleAbort);
			return result;
		} catch (error: any) {
			if (error instanceof AbortError) {
				throw error;
			}
			lastError = error;
		}
		await delay(delayInterval, options);
	}

	if (aborted) {
		throw new AbortError();
	}

	throw lastError ?? createTimeoutError(options);
};
