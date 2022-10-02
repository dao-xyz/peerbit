/**
 * Interface for G-Set CRDT
 *
 * From:
 * "A comprehensive study of Convergent and Commutative Replicated Data Types"
 * https://hal.inria.fr/inria-00555588
 */
export abstract class GSet {
  constructor(values?) { } // eslint-disable-line
  append(value) { throw new Error("Not implemented") }
  merge(set) { throw new Error("Not implemented") }
  get(value) { throw new Error("Not implemented") }
  has(value) { throw new Error("Not implemented") }
  get values(): any { throw new Error("Not implemented") }
  get length(): number { throw new Error("Not implemented") }
}

