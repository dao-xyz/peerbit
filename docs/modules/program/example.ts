/// [program]
/// [definition]
import { Log } from "@peerbit/log";
import { Program } from "@peerbit/program";
import { field, variant } from "@dao-xyz/borsh";

// The line below will make sure that every time the database manifest
// gets seriaized, "my-database" will prefix the serialized bytes (in UTF-8 encoding) so that peers
// who open the database (who recieve the database manifest in serialized bytes) can decode into this particular class.
@variant("my-database") // required
class MyDatabase extends Program {
	@field({ type: Log })
	log: Log<string>;
	constructor() {
		super();
	}

	async setup(): Promise<void> {
		return this.log.setup({
			canAppend: (entry) => {
				// Do logic here to determine whether this commit can be appended
				return true;
			},
		});
	}
}

/// [definition]

import { Observer, Replicator } from "@peerbit/program";
import { Peerbit } from "peerbit";

const client = await Peerbit.create();

/// [role]
// Open a program with the intention of replicating data and do services for data related tasks, as search (default behaviour)
await client.open(new MyDatabase(), { role: Replicator });

// Open a program with the intention of not doing any work
const store = await client.open(new MyDatabase(), { role: Observer });
/// [role]

/// [append]
const { entry } = await store.log.append("Hello world!");
expect(entry.payload.getValue()).toEqual("Hello world");
/// [append]

await client.stop();
