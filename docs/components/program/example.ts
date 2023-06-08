/// [program]
import { Log } from "@dao-xyz/peerbit-log";
import { Program } from "@dao-xyz/peerbit-program";
import { field } from "@dao-xyz/borsh";

/// [definition]
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

import { ObserverType, ReplicatorType } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";

const client = await Peerbit.create();

/// [role]
// Open a program with the intention of replicating data and do services for data related tasks, as search (default behaviour)
await client.open(new MyDatabase(), { role: ReplicatorType });

// Open a program with the intention of not doing any work
await client.open(new MyDatabase(), { role: ObserverType });
/// [role]

await client.stop();
