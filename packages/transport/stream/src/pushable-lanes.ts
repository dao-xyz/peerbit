// pushable-lanes.ts
// A multi-lane async pushable with starvation-free scheduling.
// Inspired by it-pushable (MIT) and extended for multi-lane fairness.
//
// Features:
// - Async iterator you can .push() into
// - N lanes (priorities) with starvation-free scheduling (Weighted Round-Robin)
// - Optional strict priority mode for legacy behavior
// - Optional high-water mark (bytes) with overflow policy
//
// Usage:
//   const p = pushableLanes<Uint8Array>({ lanes: 2 }); // default fairness = 'wrr'
//   p.push(new Uint8Array([1]), 1); // slower lane
//   p.push(new Uint8Array([0]), 0); // faster lane
//   for await (const chunk of p) { ... }
//
//   // Backpressure example (throw if > 8MB buffered):
//   const q = pushableLanes<Uint8Array>({ lanes: 3, maxBufferedBytes: 8 * 1024 * 1024, overflow: 'throw' });
//
// Notes:
// - T must have a .byteLength number property (e.g. Uint8Array).
// - Lane indices are 0..(lanes-1). Defaults to lane 0.
import GenericFIFO from "fast-fifo";
import defer from "p-defer";

// -----------------------------
// Errors & shared option types
// -----------------------------

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

// -----------------------------
// Public API interfaces
// -----------------------------

/**
 * An iterable that you can push values into.
 */
export interface PushableLanes<T, R = void, N = unknown>
	extends AsyncGenerator<T, R, N> {
	/**
	 * End the iterable after all values in the buffer (if any) have been yielded.
	 * If an error is passed, the buffer is cleared immediately and the next
	 * iteration will throw the passed error.
	 */
	end(err?: Error): this;

	/**
	 * Push a value into the iterable. Values are yielded in a lane-aware order.
	 * Values not yet consumed are buffered. Optional `lane` is 0-based (default 0).
	 */
	push(value: T, lane?: number): this;

	/**
	 * Resolves when the underlying buffer becomes empty (no queued data).
	 * If an AbortSignal is given and it aborts, only this promise rejects;
	 * the pushable itself is not ended.
	 */
	onEmpty(options?: AbortOptions): Promise<void>;

	/** Total number of bytes buffered (across all lanes). */
	get readableLength(): number;

	/**
	 * Get readable length for a specific lane (bytes) or total when `lane` is undefined.
	 */
	getReadableLength(lane?: number): number;
}

/** How to distribute turns between lanes. */
export type FairnessMode = "priority" | "wrr";

/** What to do when buffer would exceed `maxBufferedBytes`. */
export type OverflowPolicy = "throw" | "drop-newest";

export interface Options {
	/**
	 * Called after *all* values have been yielded from the iterator (including buffered values).
	 * If the iterator is ended with an error it will receive the error.
	 */
	onEnd?(err?: Error): void;

	/**
	 * Number of lanes. Lane 0 is the "fastest"/most preferred lane.
	 * Default: 1
	 */
	lanes?: number;

	/**
	 * Optional hook invoked on every successful push with the value and lane.
	 * Useful for metrics/telemetry.
	 */
	onPush?(value: { byteLength: number }, lane: number): void;

	/**
	 * Fairness mode:
	 *  - 'priority': strict priority (original behavior).
	 *  - 'wrr': weighted round-robin (starvation-free).
	 * Default: 'wrr'
	 */
	fairness?: FairnessMode;

	/**
	 * Weights per lane if fairness === 'wrr'. Larger weight = more service.
	 * Must have length === lanes and each weight >= 1.
	 * If omitted, weights are auto-generated from `bias`.
	 */
	weights?: number[];

	/**
	 * Bias factor for auto-generated weights when fairness === 'wrr'.
	 * For lanes L, weight[i] = floor(bias^(L-1-i)) with a minimum of 1.
	 * Default: 2 (e.g., lanes=4 -> [8,4,2,1])
	 */
	bias?: number;

	/**
	 * Optional high-water mark in **bytes** across all lanes.
	 * If a `push` would exceed this many buffered bytes:
	 *  - overflow: 'throw'       -> throw an Error (default policy)
	 *  - overflow: 'drop-newest' -> silently drop this pushed item
	 */
	maxBufferedBytes?: number;

	/** Overflow policy when `maxBufferedBytes` would be exceeded. Default: 'throw' */
	overflow?: OverflowPolicy;
}

// -----------------------------
// Internal queue primitives
// -----------------------------

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
 * FIFO that tracks the total readable bytes (`.size`) of queued values.
 */
class ByteFifo<T extends { byteLength: number }> extends GenericFIFO<Next<T>> {
	size = 0;

	push(val: Next<T>): void {
		if (val.value) this.size += val.value.byteLength;
		return super.push(val);
	}

	shift(): Next<T> | undefined {
		const shifted = super.shift();
		if (shifted?.value) this.size -= shifted.value.byteLength;
		return shifted;
	}
}

/**
 * A multi-lane queue with configurable fairness.
 * - 'priority': probe lanes in order 0..L-1 each shift.
 * - 'wrr': service lanes according to weights in a repeating schedule.
 */
class LaneQueue<T extends { byteLength: number }> {
	public readonly lanes: ByteFifo<T>[];
	private readonly mode: FairnessMode;
	private readonly schedule: number[]; // WRR: repeated lane indices per weight
	private cursor = 0;

	constructor(init: {
		lanes: number;
		fairness?: FairnessMode;
		weights?: number[];
		bias?: number;
	}) {
		const L = Math.max(1, init.lanes | 0);
		this.mode = init.fairness ?? "wrr";
		this.lanes = Array.from({ length: L }, () => new ByteFifo<T>());

		if (this.mode === "wrr") {
			const bias = init.bias ?? 2;
			const auto = Array.from({ length: L }, (_, i) =>
				Math.max(1, Math.floor(Math.pow(bias, L - 1 - i))),
			);
			const w = normalizeWeights(init.weights ?? auto, L);

			// Build a simple round-robin schedule by repeating lanes according to weight.
			this.schedule = [];
			for (let i = 0; i < L; i++) {
				for (let k = 0; k < w[i]; k++) this.schedule.push(i);
			}
			// Edge case: if all weights collapsed to zero (shouldn't), fall back to priority.
			if (this.schedule.length === 0) {
				this.schedule = Array.from({ length: L }, (_, i) => i);
			}
		} else {
			// strict priority
			this.schedule = Array.from({ length: L }, (_, i) => i);
		}
	}

	get size(): number {
		let sum = 0;
		for (const lane of this.lanes) sum += lane.size;
		return sum;
	}

	/** Enqueue a value into a specific lane. */
	push(val: Next<T>, lane: number): void {
		const idx = clampLane(lane, this.lanes.length);
		this.lanes[idx].push(val);
	}

	/** True if all lanes are empty. */
	isEmpty(): boolean {
		for (const lane of this.lanes) if (!lane.isEmpty()) return false;
		return true;
	}

	/**
	 * Dequeue the next value by fairness rules.
	 * Ensures progress even if some schedule slots map to empty lanes.
	 */
	shift(): Next<T> | undefined {
		if (this.isEmpty()) return undefined;

		if (this.mode === "priority") {
			// strict priority: always scan from lane 0
			for (let i = 0; i < this.lanes.length; i++) {
				const item = this.lanes[i].shift();
				if (item) return item;
			}
			return undefined;
		}

		// WRR mode: use rotating schedule
		const L = this.schedule.length;
		for (let probes = 0; probes < L; probes++) {
			const laneIdx = this.schedule[this.cursor];
			this.cursor = (this.cursor + 1) % L;

			const item = this.lanes[laneIdx].shift();
			if (item) return item;
		}

		// (very unlikely) nothing was found despite size>0 – linear scan fallback
		for (let i = 0; i < this.lanes.length; i++) {
			const item = this.lanes[i].shift();
			if (item) return item;
		}
		return undefined;
	}
}

function normalizeWeights(weights: number[], lanes: number): number[] {
	if (weights.length !== lanes) {
		throw new Error(
			`weights length (${weights.length}) must equal lanes (${lanes})`,
		);
	}
	const w = weights.map((x) => (x && x > 0 ? Math.floor(x) : 0));
	if (w.every((x) => x === 0)) {
		// ensure at least 1 for all lanes to retain progress guarantees
		return Array.from({ length: lanes }, () => 1);
	}
	return w;
}

function clampLane(lane: number, lanes: number): number {
	if (!Number.isFinite(lane)) return 0;
	lane = lane | 0;
	if (lane < 0) return 0;
	if (lane >= lanes) return lanes - 1;
	return lane;
}

// -----------------------------
// Factory
// -----------------------------

export function pushableLanes<T extends { byteLength: number } = Uint8Array>(
	options: Options = {},
): PushableLanes<T> {
	return _pushable<Uint8Array, T, PushableLanes<T>>(options);
}

// -----------------------------
// Core implementation
// -----------------------------
// Based on it-pushable, adapted to multi-lane buffered queues with fairness.
// Important invariants:
// - We resolve the internal "drain" promise whenever the buffer *becomes empty*.
// - After end(err), the iterator finishes; push() becomes a no-op.

function _pushable<PushType extends Uint8Array, ValueType, ReturnType>(
	options?: Options,
): ReturnType {
	options = options ?? {};
	let onEnd = options.onEnd;

	// Main buffer: multi-lane with fairness
	let buffer: LaneQueue<PushType> | ByteFifo<PushType> =
		new LaneQueue<PushType>({
			lanes: options.lanes ?? 1,
			fairness: options.fairness ?? "wrr",
			weights: options.weights,
			bias: options.bias ?? 2,
		});

	// After end(err) we may swap buffer to a simple ByteFifo to deliver the terminal signal/error.
	const isLaneQueue = (buffer: any): buffer is LaneQueue<PushType> =>
		buffer instanceof LaneQueue;

	let pushable: any;
	let onNext: ((next: Next<PushType>, lane: number) => ReturnType) | null;
	let ended = false;
	let drain = defer<void>();

	const maxBytes = options.maxBufferedBytes;
	const overflow: OverflowPolicy = options.overflow ?? "throw";

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
			value: next.value,
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
					} catch (err: any) {
						reject(err);
					}
					return pushable;
				};
			});
		} finally {
			// If buffer is empty after this turn, resolve the drain promise (in a microtask)
			if (buffer.isEmpty()) {
				queueMicrotask(() => {
					drain.resolve();
					drain = defer<void>();
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
		// swap to ByteFifo to deliver a single terminal error
		buffer = new ByteFifo<PushType>();
		if (onNext != null) {
			return onNext({ error: err }, 0);
		}
		buffer.push({ error: err });
		return pushable;
	};

	const totalBufferedBytes = (): number => {
		if (isLaneQueue(buffer)) return buffer.size;
		return (buffer as ByteFifo<PushType>).size;
	};

	const push = (value: PushType, lane: number = 0): ReturnType => {
		if (ended) {
			// Ignore pushes after end() for safety (consistent with it-pushable).
			return pushable;
		}

		// Simple backpressure: enforce HWM if configured
		if (maxBytes != null && maxBytes > 0) {
			const wouldBe = totalBufferedBytes() + value.byteLength;
			if (wouldBe > maxBytes) {
				if (overflow === "drop-newest") {
					// silently drop this item
					return pushable;
				}
				// default 'throw'
				throw new Error(
					`pushableLanes buffer overflow: ${wouldBe} bytes > maxBufferedBytes=${maxBytes}`,
				);
			}
		}

		const out = bufferNext({ done: false, value }, lane);
		options?.onPush?.(
			value,
			clampLane(lane, isLaneQueue(buffer) ? buffer.lanes.length : 1),
		);
		return out;
	};

	const end = (err?: Error): ReturnType => {
		if (ended) return pushable;
		ended = true;
		return err != null ? bufferError(err) : bufferNext({ done: true }, 0);
	};

	const _return = (): DoneResult => {
		// Ensure prompt termination
		buffer = new ByteFifo<PushType>();
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
			return totalBufferedBytes();
		},

		getReadableLength(lane?: number): number {
			if (lane == null) return totalBufferedBytes();
			if (isLaneQueue(buffer)) {
				const idx = clampLane(lane, buffer.lanes.length);
				return buffer.lanes[idx].size;
			}
			// After end/error we swap to a ByteFifo: only "total" makes sense.
			return (buffer as ByteFifo<PushType>).size;
		},

		onEmpty: async (opts?: AbortOptions) => {
			const signal = opts?.signal;
			signal?.throwIfAborted?.();

			if (buffer.isEmpty()) return;

			let cancel: Promise<void> | undefined;
			let listener: (() => void) | undefined;

			if (signal != null) {
				cancel = new Promise<void>((_, reject) => {
					listener = () => reject(new AbortError());
					signal.addEventListener("abort", listener!);
				});
			}

			try {
				await Promise.race([drain.promise, cancel]);
			} finally {
				if (listener != null) {
					signal?.removeEventListener("abort", listener);
				}
			}
		},
	};

	if (onEnd == null) {
		return pushable;
	}

	// Wrap with onEnd notifier
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
		end(err?: Error) {
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
		getReadableLength(lane?: number) {
			return _pushable.getReadableLength(lane);
		},
		onEmpty(opts?: AbortOptions) {
			return _pushable.onEmpty(opts);
		},
	};

	return pushable;
}
