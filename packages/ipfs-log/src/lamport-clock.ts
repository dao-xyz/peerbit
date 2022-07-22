import { field, variant } from "@dao-xyz/borsh";

@variant(0)
export class LamportClock {

  @field({ type: 'String' })
  id: string;

  @field({
    serialize: (value, writer) => {
      writer.writeU64(value);
    },
    deserialize: (reader) => {
      return reader.readU64().toNumber();
    }
  })
  time: number;

  constructor(id: string, time?: number) {
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

  static compare(a: LamportClock, b: LamportClock) {
    // Calculate the "distance" based on the clock, ie. lower or greater
    const dist = a.time - b.time

    // If the sequence number is the same (concurrent events),
    // and the IDs are different, take the one with a "lower" id
    if (dist === 0 && a.id !== b.id) return a.id < b.id ? -1 : 1

    return dist
  }
}