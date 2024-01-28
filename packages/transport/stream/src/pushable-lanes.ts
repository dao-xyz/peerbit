import defer from "p-defer";
import GenericFIFO from "fast-fifo";

export class AbortError extends Error {
	type: string;
	code: string;

	constructor(message?: string, code?: string) {
		super(message ?? "The operation was aborted");
		this.type = "aborted";
		this.code = code ?? "ABORT_ERR";
	}
}

export interface AbortOptions {
	signal?: AbortSignal;
}

/**
 * An iterable that you can push values into.
 */
export interface PushableLanes<T, R = void, N = unknown>
	extends AsyncGenerator<T, R, N> {
	/**
	 * End the iterable after all values in the buffer (if any) have been yielded. If an
	 * error is passed the buffer is cleared immediately and the next iteration will
	 * throw the passed error
	 */
	end(err?: Error): this;

	/**
	 * Push a value into the iterable. Values are yielded from the iterable in the order
	 * they are pushed. Values not yet consumed from the iterable are buffered.
	 */
	push(value: T, lane?: number): this;

	/**
	 * Returns a promise that resolves when the underlying queue becomes empty (e.g.
	 * this.readableLength === 0).
	 *
	 * If an AbortSignal is passed as an option and that signal aborts, it only
	 * causes the returned promise to reject - it does not end the pushable.
	 */
	onEmpty(options?: AbortOptions): Promise<void>;

	/**
	 * This property contains the total number of bytes in the queue ready to be read.
	 *
	 */

	get readableLength(): number;

	/**
	 * Get readable length for specific lane
	 * @param lane
	 */
	getReadableLength(lane?: number): number;
}

export interface Options {
	/**
	 * A function called after *all* values have been yielded from the iterator (including
	 * buffered values). In the case when the iterator is ended with an error it will be
	 * passed the error as a parameter.
	 */
	onEnd?(err?: Error): void;

	/**
	 * How many lanes, lane 0 is fastest and will drain before lane 1 is consumed
	 */
	lanes?: number;
}

export interface DoneResult {
	done: true;
}
export interface ValueResult<T> {
	done: false;
	value: T;
}
export type NextResult<T> = ValueResult<T> | DoneResult;

export interface Next<T> {
	done?: boolean;
	error?: Error;
	value?: T;
}

/**
 * Fifo but with total readableLength counter
 */
class Uint8ArrayFifo<T extends { byteLength: number }> extends GenericFIFO<
	Next<T>
> {
	size: number = 0;
	push(val: Next<T>): void {
		if (val.value) {
			this.size += val.value.byteLength;
		}
		return super.push(val);
	}

	shift(): Next<T> | undefined {
		const shifted = super.shift();
		if (shifted?.value) {
			this.size -= shifted.value.byteLength;
		}
		return shifted;
	}
}

/**
 * A queue consisting of multiple 'lanes' with different priority to be emptied.
 * The lane with index 0 will empty before lane with index 1 etc..
 * TODO add an additional proprty to control whether we  we pick objects from slower lanes
 * so no lane get really "stuck"
 */
class Uint8arrayPriorityQueue<T extends { byteLength: number }> {
	lanes: Uint8ArrayFifo<T>[];
	constructor(options: { lanes: number } = { lanes: 1 }) {
		this.lanes = new Array(options.lanes);
		for (let i = 0; i < this.lanes.length; i++) {
			this.lanes[i] = new Uint8ArrayFifo();
		}
	}

	get size() {
		let sum = 0;
		for (const lane of this.lanes) {
			sum += lane.size;
		}
		return sum;
	}
	push(val: Next<T>, lane: number) {
		return this.lanes[lane].push(val);
	}
	shift(): Next<T> | undefined {
		// fetch the first non undefined item.
		// by iterating from index 0 up we define that lanes with lower index have higher prioirity
		for (const lane of this.lanes) {
			const element = lane.shift();
			if (element) {
				return element;
			}
		}
		return undefined;
	}
	isEmpty(): boolean {
		for (const lane of this.lanes) {
			if (!lane.isEmpty()) {
				return false;
			}
		}
		return true;
	}
}

export function pushableLanes<T extends { byteLength: number } = Uint8Array>(
	options: Options = {}
): PushableLanes<T> {
	return _pushable<Uint8Array, T, PushableLanes<T>>(options);
}

// Modified from https://github.com/alanshaw/it-pushable
function _pushable<PushType extends Uint8Array, ValueType, ReturnType>(
	options?: Options
): ReturnType {
	options = options ?? {};
	let onEnd = options.onEnd;
	let buffer: Uint8arrayPriorityQueue<PushType> | Uint8ArrayFifo<PushType> =
		new Uint8arrayPriorityQueue<PushType>(
			options.lanes ? { lanes: options.lanes } : undefined
		);
	let pushable: any;
	let onNext: ((next: Next<PushType>, lane: number) => ReturnType) | null;
	let ended: boolean;
	let drain = defer();

	const getNext = (): NextResult<ValueType> => {
		const next: Next<PushType> | undefined = buffer.shift();

		if (next == null) {
			return { done: true };
		}

		if (next.error != null) {
			throw next.error;
		}

		return {
			done: next.done === true,
			// @ts-expect-error if done is false, value will be present
			value: next.value
		};
	};

	const waitNext = async (): Promise<NextResult<ValueType>> => {
		try {
			if (!buffer.isEmpty()) {
				return getNext();
			}

			if (ended) {
				return { done: true };
			}

			return await new Promise<NextResult<ValueType>>((resolve, reject) => {
				onNext = (next: Next<PushType>, lane: number) => {
					onNext = null;
					buffer.push(next, lane);

					try {
						resolve(getNext());
					} catch (err) {
						reject(err);
					}

					return pushable;
				};
			});
		} finally {
			if (buffer.isEmpty()) {
				// settle promise in the microtask queue to give consumers a chance to
				// await after calling .push
				queueMicrotask(() => {
					drain.resolve();
					drain = defer();
				});
			}
		}
	};

	const bufferNext = (next: Next<PushType>, lane: number): ReturnType => {
		if (onNext != null) {
			return onNext(next, lane);
		}

		buffer.push(next, lane);
		return pushable;
	};

	const bufferError = (err: Error): ReturnType => {
		buffer = new Uint8ArrayFifo();

		if (onNext != null) {
			return onNext({ error: err }, 0);
		}

		buffer.push({ error: err });
		return pushable;
	};

	const push = (value: PushType, lane: number = 0): ReturnType => {
		if (ended) {
			return pushable;
		}

		return bufferNext({ done: false, value }, lane);
	};
	const end = (err?: Error): ReturnType => {
		if (ended) return pushable;
		ended = true;

		return err != null ? bufferError(err) : bufferNext({ done: true }, 0);
	};
	const _return = (): DoneResult => {
		buffer = new Uint8ArrayFifo();
		end();

		return { done: true };
	};
	const _throw = (err: Error): DoneResult => {
		end(err);

		return { done: true };
	};

	pushable = {
		[Symbol.asyncIterator]() {
			return this;
		},
		next: waitNext,
		return: _return,
		throw: _throw,
		push,
		end,
		get readableLength(): number {
			return buffer.size;
		},

		getReadableLength(lane?: number): number {
			if (lane == null) {
				return buffer.size;
			}

			if (buffer instanceof Uint8arrayPriorityQueue) {
				return buffer.lanes[lane].size;
			}
			return buffer.size; // we can only arrive here if we are "done" or "err" or "end" where we reasign the buffer to a simple one and put 1 message into it
		},
		onEmpty: async (options?: AbortOptions) => {
			const signal = options?.signal;
			signal?.throwIfAborted();

			if (buffer.isEmpty()) {
				return;
			}

			let cancel: Promise<void> | undefined;
			let listener: (() => void) | undefined;

			if (signal != null) {
				cancel = new Promise((resolve, reject) => {
					listener = () => {
						reject(new AbortError());
					};

					signal.addEventListener("abort", listener);
				});
			}

			try {
				await Promise.race([drain.promise, cancel]);
			} finally {
				if (listener != null && signal != null) {
					signal?.removeEventListener("abort", listener);
				}
			}
		}
	};

	if (onEnd == null) {
		return pushable;
	}

	const _pushable = pushable;

	pushable = {
		[Symbol.asyncIterator]() {
			return this;
		},
		next() {
			return _pushable.next();
		},
		throw(err: Error) {
			_pushable.throw(err);

			if (onEnd != null) {
				onEnd(err);
				onEnd = undefined;
			}

			return { done: true };
		},
		return() {
			_pushable.return();

			if (onEnd != null) {
				onEnd();
				onEnd = undefined;
			}

			return { done: true };
		},
		push,
		end(err: Error) {
			_pushable.end(err);

			if (onEnd != null) {
				onEnd(err);
				onEnd = undefined;
			}

			return pushable;
		},
		get readableLength() {
			return _pushable.readableLength;
		},
		onEmpty: (opts?: AbortOptions) => {
			return _pushable.onEmpty(opts);
		}
	};

	return pushable;
}
