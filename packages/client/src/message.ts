import { variant, field } from "@dao-xyz/borsh";
import { ProtocolMessage } from "@dao-xyz/peerbit-program";
import { v4 as uuid } from "uuid";
@variant(1)
export abstract class TransportMessage extends ProtocolMessage {
    @field({ type: "string" })
    id: string;

    constructor() {
        super();
        this.id = uuid();
    }
}
