import { field, option, variant, vec } from "@dao-xyz/borsh";

import { BinaryPayload } from "@dao-xyz/bpayload";


export class ResultCoordinates { }

export class Result { }

@variant(0)
export class Results {
    @field({ type: vec(Result) })
    results: Result[]

    constructor(properties?: { results: Result[] }) {
        if (properties) {
            this.results = properties.results;
        }
    }
}

@variant(0)
export class ResultWithSource extends Result {

    @field({ type: BinaryPayload })
    source: BinaryPayload;

    @field({ type: option(ResultCoordinates) })
    coordinates: ResultCoordinates | undefined;

    constructor(
        opts?: {
            source: BinaryPayload,
            coordinates?: ResultCoordinates
        }
    ) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
}

