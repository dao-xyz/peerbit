import { IterationRequest } from "@peerbit/document-interface";
import {
	type Index,
	type IndexIterator,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import { ResumableIterators } from "../src/resumable-iterator.js";

describe("ResumableIterators", () => {
	it("applies deduplicated marks retained before the first fetch", async () => {
		type Row = { id: string };
		const events: string[] = [];
		const iterator: IndexIterator<Row, undefined> = {
			next: () => {
				events.push("next");
				return [];
			},
			all: () => [],
			done: () => false,
			pending: () => 0,
			close: () => undefined,
			markYielded: (ids) => {
				events.push(`mark:${[...ids].map((id) => id.primitive).join(",")}`);
			},
		};
		const index = {
			iterate: () => iterator,
		} as unknown as Index<Row>;
		const resumable = new ResumableIterators(index);
		const request = new IterationRequest({ fetch: 1 });
		const claimed = toId("claimed");

		await resumable.markYielded(request.idString, [claimed, claimed]);
		await resumable.iterateAndFetch(request, { keepAlive: true });

		expect(events).to.deep.equal(["mark:claimed", "next"]);
		expect(resumable.has(request.idString)).to.be.true;
		resumable.close(request);
	});
});
