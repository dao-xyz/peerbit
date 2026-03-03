import { Cache } from "@peerbit/cache";
import { expect } from "chai";
import sinon from "sinon";
import {
	RequestMaybeSync,
	RequestMaybeSyncCoordinate,
	SimpleSyncronizer,
} from "../src/sync/simple.js";

describe("sync-chunking", () => {
	it("chunks hash maybe-sync messages", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 2,
			},
		});

		const entries = new Map<string, any>();
		for (let i = 0; i < 5; i++) {
			entries.set(`h${i}`, { hash: `h${i}` });
		}

		await sync.onMaybeMissingEntries({
			entries: entries as any,
			targets: ["p"],
		});

		expect(send.callCount).to.equal(3);
		const sentHashes = send.getCalls().map((call) => {
			const message = call.args[0];
			expect(message).to.be.instanceOf(RequestMaybeSync);
			return (message as RequestMaybeSync).hashes;
		});
		expect(sentHashes.flat()).to.deep.equal(["h0", "h1", "h2", "h3", "h4"]);
		expect(sentHashes.map((x) => x.length)).to.deep.equal([2, 2, 1]);
	});

	it("chunks coordinate maybe-sync requests", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: { count: async () => 0 } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleCoordinatesPerMessage: 2,
			},
		});

		await sync.queueSync(
			[1n, 2n, 3n, 4n, 5n],
			{
				hashcode: () => "peer-a",
				equals: () => false,
			} as any,
			{ skipCheck: true },
		);

		expect(send.callCount).to.equal(3);
		const sentCoordinates = send.getCalls().map((call) => {
			const message = call.args[0];
			expect(message).to.be.instanceOf(RequestMaybeSyncCoordinate);
			return (message as RequestMaybeSyncCoordinate).hashNumbers;
		});
		expect(sentCoordinates.flat()).to.deep.equal([1n, 2n, 3n, 4n, 5n]);
		expect(sentCoordinates.map((x) => x.length)).to.deep.equal([2, 2, 1]);
	});
});
