import { field, BinaryWriter, vec, variant } from "@dao-xyz/borsh";
import { BinaryDocumentStore } from "@dao-xyz/peerbit-ddoc";
import { SystemBinaryPayload } from "@dao-xyz/bpayload";


// bootstrap info 

// user posts bootstrap addesses 
// reciever will connnect to these, 
// and open the network, then ask network if user is trusted 
// then save 

@variant(5)
export class DiscoveryData extends SystemBinaryPayload { }

@variant(0)
export class NetworInfo extends DiscoveryData {

    @field({ type: 'string' })
    id: string

    @field({ type: 'string' })
    network: string

    @field({ type: 'string' })
    peerId: string;

    @field({ type: vec('string') })
    addresses: string[]

    constructor(options?: {
        networkCID: string,
        peerId: string,
        addresses: string[]
    }) {
        super();
        if (options) {
            this.network = options.networkCID;
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
        writer.writeString(this.network)
        writer.writeString(this.peerId)
        return Buffer.from(writer.toArray()).toString('base64')
    }

    initialize(): NetworInfo {
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



export const createDiscoveryStore = (props: { name?: string, queryRegion?: string }) => new BinaryDocumentStore<NetworInfo>({
    indexBy: 'id',
    name: props?.name ? props?.name : '' + '_relation',
    objectType: NetworInfo.name,
    queryRegion: props.queryRegion,
    clazz: NetworInfo
})