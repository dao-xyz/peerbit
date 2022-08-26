import { field, vec, variant } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry';
import { U64Serializer } from '@dao-xyz/io-utils';

@variant(0)
export class Snapshot {

    @field({ type: 'String' })
    id: string

    @field({ type: vec(Entry) })
    heads: Entry<any>[]

    @field(U64Serializer)
    size: number

    @field({ type: vec(Entry) })
    values: Entry<any>[]

    @field({ type: 'String' })
    type: string;


    constructor(props?: {
        id: string
        heads: Entry<any>[]
        size: number
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

