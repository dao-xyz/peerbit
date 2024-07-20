/// [program]
/// [definition]
import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { type ReplicationOptions, SharedLog } from "@peerbit/shared-log";
import assert from "node:assert";
/// [definition]
import { Peerbit } from "peerbit";

// The line below will make sure that every time the database manifest
// gets serialized, "my-database" will prefix the serialized bytes (in UTF-8 encoding) so that peers
// who open the database (who receive the database manifest in serialized bytes) can decode into this particular class.

// We define an type here that is used as opening argument
// role defines the responsibilities for replicating the data
type Args = { replicate: ReplicationOptions };

@variant("my-database") // required
class MyDatabase extends Program<Args> {
	@field({ type: SharedLog })
	log: SharedLog<Uint8Array>;
	constructor() {
		super();
	}

	async open(args?: Args): Promise<void> {
		return this.log.open({ replicate: args?.replicate });
	}
}

const client = await Peerbit.create();

/// [role]
/*
Open a program with the intention of replicating data and do services for data related tasks, as search (default behaviour)
you can also do  

await client.open(new MyDatabase(), { args: { replicate: false } });
	
to not participate in replication work
*/
await client.open(new MyDatabase(), { args: { replicate: true } });

// Open a program with the intention of not doing any work
const store = await client.open(new MyDatabase(), {
	args: { replicate: false },
});
/// [role]

/// [append]
const { entry } = await store.log.append(new Uint8Array([1, 2, 3]));
assert.equal(entry.getPayloadValue(), new Uint8Array([1, 2, 3]));
/// [append]

await client.stop();
