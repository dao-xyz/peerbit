/**
 * Interface for G-Set CRDT
 *
 * From:
 * "A comprehensive study of Convergent and Commutative Replicated Data Types"
 * https://hal.inria.fr/inria-00555588
 */
export abstract class GSet {
    constructor(values?: any) {} // eslint-disable-line
    append(value: any) {
        throw new Error("Not implemented");
    }
    merge(set: any) {
        throw new Error("Not implemented");
    }
    get(value: any) {
        throw new Error("Not implemented");
    }
    has(value: any) {
        throw new Error("Not implemented");
    }
    get values(): any {
        throw new Error("Not implemented");
    }
    get length(): number {
        throw new Error("Not implemented");
    }
}
