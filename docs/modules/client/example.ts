/// [memory]
import { Peerbit } from "peerbit";

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
