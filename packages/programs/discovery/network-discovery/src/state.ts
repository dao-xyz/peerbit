import { field, BinaryWriter, vec, variant } from "@dao-xyz/borsh";
import { Documents, DocumentIndex } from "@dao-xyz/peerbit-document";
import { SystemBinaryPayload } from "@dao-xyz/peerbit-bpayload";
import { Address } from "@dao-xyz/peerbit-store";
import { AnySearch } from "@dao-xyz/peerbit-anysearch";
import { DQuery } from "@dao-xyz/peerbit-query";
import { createHash } from "crypto";


// bootstrap info 

// user posts bootstrap addesses 
// reciever will connnect to these, 
// and open the network, then ask network if user is trusted 
// then save 

@variant(5)
export class DiscoveryData extends SystemBinaryPayload { }

@variant(0)
export class NetworkInfo extends DiscoveryData {

    @field({ type: 'string' })
    id: string

    @field({ type: Address })
    network: Address

    @field({ type: 'string' })
    peerId: string;

    @field({ type: vec('string') })
    addresses: string[]

    constructor(options?: {
        network: Address,
        peerId: string,
        addresses: string[]
    }) {
        super();
        if (options) {
            this.network = options.network;
            this.peerId = options.peerId;
            this.addresses = options.addresses;
            this.initialize();
        }
    }

    calculateId(): string {
        if (!this.network || !this.peerId) {
            throw new Error("Not initialized");
        }
        const writer = new BinaryWriter();
        writer.writeString(this.network.toString())
        writer.writeString(this.peerId)
        return createHash('sha1').update(writer.toArray()).digest('base64')
    }

    initialize(): NetworkInfo {
        this.id = this.calculateId();
        return this;
    }

    assertId() {
        const calculatedId = this.calculateId();
        if (this.id !== calculatedId) {
            throw new Error(`Invalid id, got ${this.id} but expected ${calculatedId}`)
        }
    }
}



export const createDiscoveryStore = (props?: { id?: string, queryRegion?: string }) => new Documents<NetworkInfo>({
    id: props?.id ? props?.id : '' + '_discovery',
    index: new DocumentIndex({
        indexBy: 'id',
        search: new AnySearch({
            query: new DQuery({
                queryRegion: props?.queryRegion
            })
        })
    })
})