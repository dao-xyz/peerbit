import { field, variant } from "@dao-xyz/borsh";
import { U8IntArraySerializer } from "@dao-xyz/borsh-utils";
import { arraysCompare, arraysEqual } from "@dao-xyz/borsh-utils";

@variant(0)
export class LamportClock {

  @field(U8IntArraySerializer)
  id: Uint8Array;

  @field({ type: 'u64' })
  time: bigint;

  constructor(id: Uint8Array, time?: bigint | number) {
    this.id = id
    if (!time) {
      this.time = 0n;
    }
    else {
      this.time = typeof time === 'number' ? BigInt(time) : time
    }
  }

  clone() {
    return new LamportClock(this.id, this.time)
  }

  equals(other: LamportClock): boolean {
    return arraysEqual(this.id, other.id) && this.time === other.time;
  }

  advance() {
    return new LamportClock(this.id, this.time + 1n)

  }


  static compare(a: LamportClock, b: LamportClock) {
    // Calculate the "distance" based on the clock, ie. lower or greater
    const dist = a.time - b.time
    if (dist > 0) {
      return 1;
    }
    if (dist < 0) {
      return -1;
    }
    // If the sequence number is the same (concurrent events),
    // and the IDs are different, take the one with a "lower" id
    return arraysCompare(a.id, b.id);
  }
}