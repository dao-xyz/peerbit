import { variant } from "@dao-xyz/borsh";
import { ProtocolMessage } from "@dao-xyz/peerbit-program";

@variant(1)
export abstract class TransportMessage extends ProtocolMessage {}
