/// [program]
/// [definition]
import { Log } from "@dao-xyz/peerbit-log";
import { Program } from "@dao-xyz/peerbit-program";
import { field } from "@dao-xyz/borsh";

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

import { Observer, Replicator } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";

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
