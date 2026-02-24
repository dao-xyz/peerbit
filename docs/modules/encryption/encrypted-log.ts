import { field, variant } from "@dao-xyz/borsh";
import { Ed25519Keypair, X25519Keypair } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { SharedLog } from "@peerbit/shared-log";
import { waitForResolved } from "@peerbit/time";
import assert from "node:assert";
import { Peerbit } from "peerbit";

// This class extends Program which allows it to be replicated amongst peers
@variant("simple_store")
class SimpleStore extends Program {
	@field({ type: SharedLog })
	log: SharedLog<Uint8Array>; // Documents<?> provide document store functionality around your Posts

	constructor() {
		super();
		this.log = new SharedLog();
	}

	async open(): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.log.open();
	}
}

const [client, client2, thirdPartyIdentity] = await Promise.all([
	Peerbit.create(),
	Peerbit.create(),
	// We only need a public key for the third party (no libp2p node required).
	Ed25519Keypair.create(),
]);

try {
	// Dial only a direct TCP address when possible. Dialing a full multiaddr set can
	// be slow/flaky in CI if some transports are unavailable.
	const addrs = client.getMultiaddrs();
	const preferred = addrs.find((addr) => {
		const s = addr.toString();
		return s.includes("/tcp/") && !s.includes("/ws");
	});
	await client2.dial(preferred ? [preferred] : addrs);
	// In small ad-hoc networks (no bootstraps/trackers), proactively hosting shard
	// roots avoids flaky "join before root is hosted" races.
	await Promise.all([
		(client.services.pubsub as any).hostShardRootsNow?.(),
		(client2.services.pubsub as any).hostShardRootsNow?.(),
	]);

	const store = await client.open(new SimpleStore());

	const payload = new Uint8Array([1, 2, 3]);
	await store.log.append(payload, {
		encryption: {
			keypair: await X25519Keypair.create(),
			receiver: {
				// Who can read the log entry metadata (e.g. timestamps), next pointers,
				// and more location information?
				meta: [
					client.identity.publicKey,
					client2.identity.publicKey,
					thirdPartyIdentity.publicKey,
				],

				// Who can read the message payload?
				payload: [client.identity.publicKey, client2.identity.publicKey],

				// Who can read the signature?
				// (In order to validate entries you need to be able to read the signature.)
				signatures: [
					client.identity.publicKey,
					client2.identity.publicKey,
					thirdPartyIdentity.publicKey,
				],
			},
		},
	});

	// A peer that can open.
	const store2 = await client2.open<SimpleStore>(store.address!);
	await waitForResolved(() => assert.equal(store2.log.log.length, 1), {
		timeout: 30_000,
	});
	const entry = (await store2.log.log.toArray())[0];

	// Use .getPayloadValue() instead of .payload to decrypt the payload.
	assert.deepEqual(await entry.getPayloadValue(), payload);
} finally {
	await Promise.allSettled([client.stop(), client2.stop()]);
}
