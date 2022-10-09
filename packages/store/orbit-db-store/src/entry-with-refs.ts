import { Entry } from "@dao-xyz/ipfs-log";
import { field, variant, vec } from '@dao-xyz/borsh';


/**
 * This thing allows use to faster sync since we can provide 
 * references that can be read concurrently to 
 * the entry when doing Log.fromEntry or Log.fromEntryHash
 */
@variant(0)
export class EntryWithRefs<T> {
    @field({ type: Entry })
    entry: Entry<T>

    @field({ type: vec(Entry) })
    references: Entry<T>[] // are parents to entry. 


    constructor(properties?: { entry: Entry<T>, references: Entry<T>[] }) {
        if (properties) {
            this.entry = properties.entry;
            this.references = properties.references;
        }
    }
}