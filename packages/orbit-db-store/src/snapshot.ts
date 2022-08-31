import { field, vec, variant } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry';

@variant(0)
export class Snapshot {

    @field({ type: 'string' })
    id: string

    @field({ type: vec(Entry) })
    heads: Entry<any>[]

    @field({ type: 'u64' })
    size: bigint

    @field({ type: vec(Entry) })
    values: Entry<any>[]

    @field({ type: 'string' })
    type: string;


    constructor(props?: {
        id: string
        heads: Entry<any>[]
        size: bigint
        values: Entry<any>[]
        type: string;
    }) {
        if (props) {
            this.heads = props.heads;
            this.id = props.id;
            this.size = props.size;
            this.values = props.values;
            this.type = props.type;
        }
    }
}

