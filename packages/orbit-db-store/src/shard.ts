
import { variant, field } from '@dao-xyz/borsh';
/* 
export class Sharding {
    _requestStore: (store: Any) => void;
    init(requestNewShard: () => void) {
        this._requestNewShard = requestNewShard;
    }

    onMemoryExceeded(store: Freezable): void {
        throw new Error("Not implemented")
    }
}

@variant(0)
export class NoSharding extends Sharding {
    onMemoryExceeded(store: Freezable): void {
        return;
    }
}

@variant(1)
export class ShardingCounter extends Sharding {

    @field({ type: 'u64' })
    shardIndex: bigint;

    constructor(properties?: { shardIndex: bigint }) {
        super();
        if (properties) {
            this.shardIndex = properties.shardIndex;
        }
    }

    onMemoryExceeded(store: Freezable): void {
        store.freeze();
        this._requestNewShard();
    }
}


 */

/* export interface Freezable {
    freeze(): void;
} */