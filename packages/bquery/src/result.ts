import { field, option, variant } from "@dao-xyz/borsh";


export class ResultSource { }

export class ResultCoordinates { }

export class Result { }

@variant(0)
export class ResultWithSource extends Result {

    @field({ type: ResultSource })
    source: ResultSource;

    @field({ type: option(ResultCoordinates) })
    coordinates: ResultCoordinates | undefined;

    constructor(
        opts?: {
            source: ResultSource,
            coordinates?: ResultCoordinates
        }
    ) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
}

