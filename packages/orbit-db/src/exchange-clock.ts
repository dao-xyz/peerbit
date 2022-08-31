import { Message } from "./message";
/* import { Message } from "./message";
import { variant, option, field } from '@dao-xyz/borsh';
import { LamportClock } from "@dao-xyz/ipfs-log-entry";
import { bufferSerializer } from "@dao-xyz/encryption-utils";
import { X25519PublicKey } from 'sodium-plus';

@variant([2, 0])
export class ExchangeClockMessage extends Message {

}

@variant(0)
export class RequestLamportClock extends ExchangeClockMessage {

    @field({ type: 'string' })
    replicationTopic: string;

    @field({ type: 'string' })
    address: string;

    @field({ type: option(bufferSerializer(X25519PublicKey)) })
    encryptionKey?: X25519PublicKey



    constructor(props?: {
        replicationTopic: string,
        address: string,
        encryptionKey?: X25519PublicKey
    }) {
        super();
        if (props) {
            this.replicationTopic = props.replicationTopic;
            this.address = props.address;
            this.encryptionKey = props.encryptionKey;
        }
    }
}

@variant(1)
export class ExchangeLamportClock extends ExchangeClockMessage {


    @field({ type: 'string' })
    replicationTopic: string;

    @field({ type: 'string' })
    address: string;

    @field({ type: LamportClock })
    clock: LamportClock

    constructor(props?: { clock: LamportClock, replicationTopic: string, address: string }) {
        super();
        if (props) {
            this.replicationTopic = props.replicationTopic;
            this.clock = props.clock;
            this.address = props.address;
        }
    }
} */