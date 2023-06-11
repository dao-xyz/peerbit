/// [imports]
import { Peerbit } from "@dao-xyz/peerbit";
/// [imports]

/// [memory]

// The most important configuration is to determine whether you want data to persist between session, or if you want it to persist in memory only
// Data is not only things that you generate in your databases, but also keys that are used to encrypt and decrypt them.

// Store only in memory
const clientInMemory = await Peerbit.create();

// Store content on disc when possible
const clientWithStorage = await Peerbit.create({
	directory: "/path/somewhere",
});
/// [memory]

/// [stop]
await clientInMemory.stop();
/// [stop]

await clientWithStorage.stop();
