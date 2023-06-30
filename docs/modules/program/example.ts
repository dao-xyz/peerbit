/// [program]
/// [definition]
import { Program } from "@peerbit/program";
import { field, variant } from "@dao-xyz/borsh";
import { Observer, Replicator, SharedLog, Role } from "@peerbit/shared-log";

// The line below will make sure that every time the database manifest
// gets seriaized, "my-database" will prefix the serialized bytes (in UTF-8 encoding) so that peers
// who open the database (who recieve the database manifest in serialized bytes) can decode into this particular class.

type Args = { role: Role };

@variant("my-database") // required
class MyDatabase extends Program<Args> {
	@field({ type: SharedLog })
	log: SharedLog<Uint8Array>;
	constructor() {
		super();
	}

	async open(args?: Args): Promise<void> {
		return this.log.open({ role: args?.role });
	}
}

/// [definition]
import { Peerbit } from "peerbit";

const client = await Peerbit.create();

/// [role]
// Open a program with the intention of replicating data and do services for data related tasks, as search (default behaviour)
await client.open(new MyDatabase(), { args: { role: Replicator } });

// Open a program with the intention of not doing any work
const store = await client.open(new MyDatabase(), { args: { role: Observer } });
/// [role]

/// [append]
const { entry } = await store.log.append(new Uint8Array([1, 2, 3]));
expect(entry.getPayloadValue()).toEqual(new Uint8Array([1, 2, 3]));
/// [append]

await client.stop();
