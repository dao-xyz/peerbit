/**
The MIT License (MIT)

Copyright (c) 2021 Martin Heidegger
Copyright (c) 2022 dao.xyz

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

import { field, variant } from "@dao-xyz/borsh";
import { compare, equals } from "@peerbit/uint8arrays";
import { hrtime } from "@peerbit/time";

const hrTimeNow = hrtime.bigint();
const startTime = BigInt(Date.now()) * BigInt(1e6) - hrTimeNow;
const bigintTime = () => startTime + hrtime.bigint();

export function fromBits(low, high, unsigned, target?) {
	if (target === undefined || target === null) {
		return {
			low: low | 0,
			high: high | 0,
			unsigned: !!unsigned
		};
	}
	target.low = low | 0;
	target.high = high | 0;
	target.unsigned = !!unsigned;
	return target;
}

const n1e6 = BigInt(1e6);
const UINT64_MAX = 18446744073709551615n;
const UINT32_MAX = 0xffffffff;

function bigIntCoerce(input, fallback) {
	if (typeof input === "bigint") return input;
	if (typeof input === "number" || typeof input === "string")
		return BigInt(input);
	return fallback;
}

@variant(0)
export class Timestamp {
	@field({ type: "u64" })
	wallTime: bigint;

	@field({ type: "u32" })
	logical: number;

	constructor(properties?: { wallTime: bigint; logical?: number }) {
		if (properties) {
			this.wallTime = properties.wallTime;
			this.logical = properties.logical || 0;
		}
	}

	static compare(a: Timestamp, b: Timestamp) {
		if (a.wallTime > b.wallTime) return 1;
		if (a.wallTime < b.wallTime) return -1;
		if (a.logical > b.logical) return 1;
		if (a.logical < b.logical) return -1;
		return 0;
	}

	static bigger(a: Timestamp, b: Timestamp) {
		return a.compare(b) === -1 ? b : a;
	}
	static smaller(a: Timestamp, b: Timestamp) {
		return a.compare(b) === 1 ? b : a;
	}

	compare(other: Timestamp) {
		return Timestamp.compare(this, other);
	}
	clone(): Timestamp {
		return new Timestamp({
			wallTime: this.wallTime,
			logical: this.logical
		});
	}

	toString() {
		return `Timestamp: wallTime: ${this.wallTime}, logical: ${this.logical}`;
	}
}

export class HLC {
	maxOffset: bigint;
	wallTimeUpperBound: bigint;
	toleratedForwardClockJump: bigint;
	last: Timestamp;
	wallTime: () => bigint;
	constructor(
		properties: {
			wallTime?: () => bigint;
			maxOffset?: bigint;
			wallTimeUpperBound?: bigint;
			toleratedForwardClockJump?: bigint;
			last?: Timestamp;
		} = {}
	) {
		this.wallTime = properties.wallTime || bigintTime;
		this.maxOffset = bigIntCoerce(properties.maxOffset, 0n);
		this.wallTimeUpperBound = bigIntCoerce(properties.wallTimeUpperBound, 0n);
		this.toleratedForwardClockJump = bigIntCoerce(
			properties.toleratedForwardClockJump,
			0n
		);
		this.last = new Timestamp({ wallTime: this.wallTime() });
		if (properties.last) {
			this.last = Timestamp.bigger(properties.last, this.last);
		}
	}

	now() {
		return this.update(this.last);
	}

	validateOffset(offset: bigint) {
		if (
			this.toleratedForwardClockJump > 0n &&
			-offset > this.toleratedForwardClockJump
		) {
			throw new ForwardJumpError(-offset, this.toleratedForwardClockJump);
		}
		if (this.maxOffset > 0n && offset > this.maxOffset) {
			throw new ClockOffsetError(offset, this.maxOffset);
		}
	}

	update(other: Timestamp) {
		const last = Timestamp.bigger(other, this.last);
		let wallTime = this.wallTime();
		const offset = last.wallTime - wallTime;
		this.validateOffset(offset);
		let logical: number;
		if (offset < 0n) {
			logical = 0;
		} else {
			wallTime = last.wallTime;
			logical = last.logical + 1;
			if (logical > UINT32_MAX) {
				wallTime += 1n;
				logical = 0;
			}
		}
		const maxWallTime =
			this.wallTimeUpperBound > 0n ? this.wallTimeUpperBound : UINT64_MAX;
		if (wallTime > maxWallTime) {
			throw new WallTimeOverflowError(wallTime, maxWallTime);
		}
		this.last = new Timestamp({ wallTime, logical });
		return this.last;
	}
}

export class ClockOffsetError extends Error {
	offset: bigint;
	maxOffset: bigint;
	constructor(offset: bigint, maxOffset: bigint) {
		super(
			`The received time is ${
				offset / n1e6
			}ms ahead of the wall time, exceeding the 'maxOffset' limit of ${
				maxOffset / n1e6
			}ms.`
		);
		this.offset = offset;
		this.maxOffset = maxOffset;
	}
}

export class WallTimeOverflowError extends Error {
	time: bigint;
	maxTime: bigint;
	constructor(time: bigint, maxTime: bigint) {
		super(
			`The wall time ${time / n1e6}ms exceeds the max time of ${
				maxTime / n1e6
			}ms.`
		);
		this.time = time;
		this.maxTime = maxTime;
	}
}

export class ForwardJumpError extends Error {
	timejump: bigint;
	tolerance: bigint;
	constructor(timejump: bigint, tolerance: bigint) {
		super(
			`Detected a forward time jump of ${
				timejump / n1e6
			}ms, which exceed the allowed tolerance of ${tolerance / n1e6}ms.`
		);
		this.timejump = timejump;
		this.tolerance = tolerance;
	}
}

@variant(0)
export class LamportClock {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Timestamp })
	timestamp: Timestamp;

	constructor(properties: { id: Uint8Array; timestamp?: Timestamp | number }) {
		this.id = properties.id;
		if (!properties.timestamp) {
			this.timestamp = new Timestamp({
				wallTime: bigintTime(),
				logical: 0
			});
		} else {
			if (typeof properties.timestamp === "number") {
				this.timestamp = new Timestamp({
					wallTime: bigintTime(),
					logical: properties.timestamp
				});
			} else {
				this.timestamp = properties.timestamp;
			}
		}
	}

	clone() {
		return new LamportClock({
			id: this.id,
			timestamp: this.timestamp.clone()
		});
	}

	equals(other: LamportClock): boolean {
		return (
			equals(this.id, other.id) && this.timestamp.compare(other.timestamp) === 0
		);
	}

	/**
	 * Not optimized, dont use for performance critical things
	 * @returns
	 */
	advance() {
		const h = new HLC();
		h.update(new Timestamp(this.timestamp));
		return new LamportClock({ id: this.id, timestamp: h.now() });
	}

	static compare(a: LamportClock, b: LamportClock) {
		// Calculate the "distance" based on the clock, ie. lower or greater

		const timestamp = a.timestamp.compare(b.timestamp);
		if (timestamp !== 0) return timestamp;

		// If the sequence number is the same (concurrent events),
		// and the IDs are different, take the one with a "lower" id
		return compare(a.id, b.id);
	}
}
