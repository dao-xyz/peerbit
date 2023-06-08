/// [imports]
import { Peerbit } from "@dao-xyz/peerbit";
/// [imports]

/// [memory]
// Store only in memory
const clientInMemory = await Peerbit.create();

// Store content on disc when possible
const clientWithStorage = await Peerbit.create({
	directory: "/path/somewhere",
});
/// [memory]

// TODO idenitity config examples

/// [stop]
await clientInMemory.stop();
/// [stop]

await clientWithStorage.stop();
