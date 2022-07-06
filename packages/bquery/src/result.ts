import { field, option, variant } from "@dao-xyz/borsh";
import { BPayload } from '@dao-xyz/bgenerics';


export class ResultCoordinates { }

export class Result { }

@variant(0)
export class ResultWithSource extends Result {

    @field({ type: BPayload })
    source: BPayload;

    @field({ type: option(ResultCoordinates) })
    coordinates: ResultCoordinates | undefined;

    constructor(
        opts?: {
            source: BPayload,
            coordinates?: ResultCoordinates
        }
    ) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
}

