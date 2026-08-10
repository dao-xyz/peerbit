import { serialize } from "@dao-xyz/borsh";
import { type PublicSignKey, sha256Sync } from "@peerbit/crypto";
import { concat, fromString } from "uint8arrays";

const RECEIVER_BINDING_DOMAIN = fromString(
	"peerbit/shared-log/replication-info-v2/receiver-binding/v1",
);

const lengthPrefixed = (bytes: Uint8Array): Uint8Array => {
	const length = new Uint8Array(4);
	new DataView(length.buffer).setUint32(0, bytes.byteLength, true);
	return concat([length, bytes]);
};

const u64LittleEndian = (value: bigint): Uint8Array => {
	const bytes = new Uint8Array(8);
	new DataView(bytes.buffer).setBigUint64(0, value, true);
	return bytes;
};

/**
 * Derive the token echoed by V2 data frames. Delivery recipients are unsigned,
 * so the receiver's nonce alone is not a destination binding. Folding both
 * authenticated identities and signed transport sessions into the 32-byte
 * field prevents a copied request nonce from creating an interchangeable
 * stream for another receiver.
 */
export const deriveReplicationInfoV2ReceiverBinding = (properties: {
	receiverChallenge: Uint8Array;
	receiver: PublicSignKey;
	receiverTransportSession: bigint;
	sender: PublicSignKey;
	senderTransportSession: bigint;
}): Uint8Array =>
	sha256Sync(
		concat([
			lengthPrefixed(RECEIVER_BINDING_DOMAIN),
			lengthPrefixed(properties.receiverChallenge),
			lengthPrefixed(serialize(properties.receiver)),
			u64LittleEndian(properties.receiverTransportSession),
			lengthPrefixed(serialize(properties.sender)),
			u64LittleEndian(properties.senderTransportSession),
		]),
	);
