import { field, variant } from "@dao-xyz/borsh";

/**
 * Application-level envelope for fanout-delivered shared-log messages.
 *
 * FanoutTree's data-plane forwarding makes the root the transport-level origin
 * when proxy-publishing. Shared-log needs a stable publisher identity and
 * timestamp regardless of which overlay hop forwarded the bytes.
 */
@variant(0)
export class FanoutEnvelope {
	@field({ type: "string" })
	from: string; // publisher publicKey hash

	@field({ type: "u64" })
	timestamp: bigint; // publisher-supplied timestamp

	@field({ type: Uint8Array })
	payload: Uint8Array; // serialized TransportMessage

	constructor(props: { from: string; timestamp: bigint; payload: Uint8Array }) {
		this.from = props.from;
		this.timestamp = props.timestamp;
		this.payload = props.payload;
	}
}

