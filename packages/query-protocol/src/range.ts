import { field, variant, vec } from "@dao-xyz/borsh";
import { ResultCoordinates } from "./result";

@variant(0)
export class RangeCoordinate {

    @field({ type: 'u64' })
    offset: bigint;

    @field({ type: 'u64' })
    length: bigint;

    constructor(opts?: {
        offset: bigint;
        length: bigint;
    }) {
        if (opts) {
            Object.assign(this, opts);
        }
    }

}
@variant(0)
export class RangeCoordinates extends ResultCoordinates {

    @field({ type: vec(RangeCoordinate) })
    coordinates: RangeCoordinate[]

    constructor(opts?: {
        coordinates: RangeCoordinate[];
    }) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }

}
