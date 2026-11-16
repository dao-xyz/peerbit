import { IterationRequest } from "@peerbit/document-interface";
import {
	type Index,
	type IndexIterator,
	toId,
} from "@peerbit/indexer-interface";
import { disable, enable } from "@peerbit/logger";
import { expect } from "chai";
import sinon from "sinon";
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

	describe("iterate logger", () => {
		type Row = { id: string };

		// enable() and disable() replace the debug namespaces of the whole
		// process. Node keeps them in process.env.DEBUG, so restore that.
		let namespaces: string | undefined;
		beforeEach(() => {
			namespaces = process.env.DEBUG;
		});
		afterEach(() => {
			sinon.restore();
			enable(namespaces ?? "");
			if (namespaces === undefined) {
				delete process.env.DEBUG;
			}
		});

		const openIterator = async () => {
			const calls = { pending: 0 };
			const iterator: IndexIterator<Row, undefined> = {
				next: () => [],
				all: () => [],
				done: () => false,
				pending: () => {
					calls.pending++;
					return 7;
				},
				close: () => undefined,
			};
			const resumable = new ResumableIterators({
				iterate: () => iterator,
			} as unknown as Index<Row>);
			const request = new IterationRequest({ fetch: 1 });
			await resumable.iterateAndFetch(request, { keepAlive: true });
			return { resumable, request, calls };
		};

		it("next does not call pending when the iterate logger is disabled", async () => {
			disable();
			const { resumable, request, calls } = await openIterator();

			await resumable.next(request);
			await resumable.next(request);

			expect(calls.pending).to.equal(0);
			resumable.close(request);
		});

		it("next calls pending when the iterate logger is enabled", async () => {
			enable("peerbit:document:index:iterate");
			const write = sinon.stub(process.stderr, "write").returns(true);
			const { resumable, request, calls } = await openIterator();

			await resumable.next(request);
			resumable.close(request);
			write.restore();

			expect(calls.pending).to.equal(1);
			const logged = write.args
				.map(([chunk]) => String(chunk))
				.filter((line) => line.includes("iterate:next"));
			expect(logged).to.have.length(1);
			for (const field of [
				`id: '${request.idString}'`,
				"fetch: 1",
				"keepAlive: true",
				"resultLength: 0",
				"pending: 7",
				"done: false",
			]) {
				expect(logged[0]).to.include(field);
			}
		});
	});
});
