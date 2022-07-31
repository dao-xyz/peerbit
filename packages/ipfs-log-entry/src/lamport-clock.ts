import { field, variant } from "@dao-xyz/borsh";
import { U8IntArraySerializer } from "@dao-xyz/borsh-utils";
import { arraysCompare, arraysEqual } from "./utils";
@variant(0)
export class LamportClock {

  @field(U8IntArraySerializer)
  id: Uint8Array;

  @field({
    serialize: (value, writer) => {
      writer.writeU64(value);
    },
    deserialize: (reader) => {
      return reader.readU64().toNumber();
    }
  })
  time: number;

  constructor(id: Uint8Array, time?: number) {
    this.id = id
    this.time = time || 0
  }

  tick() {
    return new LamportClock(this.id, ++this.time)
  }

  merge(clock) {
    this.time = Math.max(this.time, clock.time)
    return new LamportClock(this.id, this.time)
  }

  clone() {
    return new LamportClock(this.id, this.time)
  }

  equals(other: LamportClock): boolean {
    return arraysEqual(this.id, other.id) && this.time === other.time;
  }


  static compare(a: LamportClock, b: LamportClock) {
    // Calculate the "distance" based on the clock, ie. lower or greater
    const dist = a.time - b.time

    // If the sequence number is the same (concurrent events),
    // and the IDs are different, take the one with a "lower" id
    if (dist === 0) {
      return arraysCompare(a.id, b.id);
    }

    return dist
  }
}