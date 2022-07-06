import { variant, field } from '@dao-xyz/borsh';
@variant(0)
export class LamportClock {

  @field({ type: 'String' })
  id: string;
  @field({
    serialize: (value, writer) => {
      writer.writeU64(value)
    },
    deserialize: (reader) => {
      return reader.readU64()
    }
  })
  time: number;

  constructor(options?: { id: string, time?: number }) {
    if (options) {
      this.id = options.id

      this.time = options.time ? options.time : 0

    }
  }

  tick() {
    this.time = this.time + 1;
  }

  merge(clock) {
    this.time = Math.max(this.time, clock.time)
    return new LamportClock({ id: this.id, time: this.time })
  }

  clone() {
    return new LamportClock({ id: this.id, time: this.time })
  }

  static compare(a: LamportClock, b: LamportClock) {
    // Calculate the "distance" based on the clock, ie. lower or greater
    const dist = a.time - b.time;

    // If the sequence number is the same (concurrent events),
    // and the IDs are different, take the one with a "lower" id
    if (dist === 0 && a.id !== b.id) return a.id < b.id ? -1 : 1

    return dist
  }
}