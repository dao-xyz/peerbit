import { waitFor } from "@dao-xyz/peerbit-time";
import { variant, field } from "@dao-xyz/borsh";
import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import {
	Documents,
	PutOperation,
	DocumentIndex,
} from "@dao-xyz/peerbit-document";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { ObserverType, Program } from "@dao-xyz/peerbit-program";
import { RPC } from "@dao-xyz/peerbit-rpc";
import { Entry } from "@dao-xyz/peerbit-log";

describe(`Subprogram`, () => {
	let session: LSession;
	let client1: Peerbit, client2: Peerbit;
	let timer: any;

	beforeAll(async () => {
		session = await LSession.connected(2);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		clearInterval(timer);

		client1 = await Peerbit.create({
			libp2p: session.peers[0],
		});
		client2 = await Peerbit.create({
			libp2p: session.peers[1],
			limitSigning: true,
		}); // limitSigning = dont sign exchange heads request
	});

	afterEach(async () => {
		clearInterval(timer);
		if (client1) await client1.stop();
		if (client2) await client2.stop();
	});

	@variant("program_with_subprogram")
	class ProgramWithSubprogram extends Program {
		@field({ type: Documents })
		eventStore: Documents<EventStore<string>>;

		accessRequests: { entry: Entry<any> }[];

		constructor(eventStore: Documents<EventStore<string>>) {
			super();
			this.eventStore = eventStore;
		}

		async canAppend(entry: Entry<any>): Promise<boolean> {
			this.accessRequests.push({ entry }); // this is wat we are testing, are we going here when opening a subprogram?
			return true;
		}

		setup(): Promise<void> {
			this.accessRequests = [];
			return this.eventStore.setup({
				type: EventStore,
				canAppend: this.canAppend.bind(this),
				canOpen: (program: Program) =>
					Promise.resolve(program.constructor === EventStore),
			});
		}
	}

	it("can open store on exchange heads message when trusted", async () => {
		const store = new ProgramWithSubprogram(
			new Documents<EventStore<string>>({
				index: new DocumentIndex({
					indexBy: "id",
					query: new RPC(),
				}),
			})
		);

		const program = await client1.open(store, {
			role: new ObserverType(),
		});
		program.accessRequests = [];

		await client2.open(program.address);

		const eventStoreToPut = new EventStore<string>({ id: "store 1" });
		const { entry: eventStore } = await store.eventStore.put(eventStoreToPut);

		const _eventStore2 = await store.eventStore.put(
			new EventStore({ id: "store 2" })
		);
		expect(store.eventStore.store.oplog.heads).toHaveLength(2); // two independent documents
		await waitFor(() => client2.programs.size == 3);
		expect(program.accessRequests).toHaveLength(2);

		const eventStoreString = (
			(await eventStore.payload.getValue()) as PutOperation<any>
		).value as EventStore<string>;

		expect(eventStoreToPut).toEqual(eventStoreString);

		await client1.open(eventStoreString, {
			role: new ObserverType(),
		});

		const eventStore2 = client2.programs.get(
			eventStoreToPut.address!.toString()
		)!.program as EventStore<string>;
		await eventStoreString.add("hello"); // This will exchange an head that will make client 1 open the store
		await waitFor(() => eventStore2.store.oplog.values.length === 1);
	});
});
