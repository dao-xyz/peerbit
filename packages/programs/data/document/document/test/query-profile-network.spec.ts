import { serialize } from "@dao-xyz/borsh";
import type { DiagnosticEvent } from "@peerbit/diagnostics";
import { Timestamp } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { Documents } from "../src/index.js";
import { Document, TestStore } from "./data.js";

describe("document query diagnostics over real RPC", function () {
	this.timeout(30_000);
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("forwards authenticated query RPC events through the open profile option", async () => {
		// Seed before connecting so fixture construction cannot accidentally query
		// a peer that has not opened this program. connect() uses real libp2p
		// neighbor streams, including real TCP in the CI fast-session preset.
		session = await TestSession.disconnected(2);
		const args = {
			mode: "compat" as const,
			nativeGraph: false as const,
			nativeBackbone: false as const,
			nativeRangePlanner: false as const,
			index: { prefetch: false as const },
		};
		const donor = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>({ immutable: true }) }),
			{ args: { ...args, replicate: { factor: 1 } } },
		);
		const document = new Document({
			id: "real-query-profile",
			name: "seeded only on donor",
			data: new Uint8Array([0, 1, 127, 128, 255]),
		});
		const encoded = serialize(document);
		const created = 1_000_000n;
		const committed = await donor.docs.put(document, {
			target: "none",
			meta: { timestamp: new Timestamp({ wallTime: created }) },
		});
		const events: DiagnosticEvent[] = [];
		const observer = await session.peers[1].open(donor.clone(), {
			args: {
				...args,
				replicate: false,
				sync: {
					profile: (event) => {
						if (
							event.name.startsWith("documents.query.") ||
							event.name.startsWith("rpc.request.")
						) {
							events.push({ ...event, details: { ...event.details } });
						}
					},
				},
			},
		});
		await session.connect();
		const donorKey = session.peers[0].identity.publicKey;
		const donorHash = donorKey.hashcode();
		// This public fence establishes both query and SharedLog topics and the
		// donor's replication-index membership; it is not a durability receipt.
		expect(
			await observer.docs.index.waitFor(donorKey, { timeout: 5_000 }),
		).deep.equal([donorHash]);
		expect(await observer.docs.index.getSize()).equal(0);
		expect(observer.docs.log.log.length).equal(0);
		events.length = 0;

		// No request stubs, response interceptors, fabricated contexts, or policy
		// bypasses: the actual request and reply pass through both RPC instances.
		const results = await observer.docs.index.getDetailed(document.id, {
			resolve: false,
			local: false,
			remote: {
				from: [donorHash],
				replicate: false,
				timeout: 5_000,
				throwOnMissing: true,
			},
		});
		expect(results).to.have.length(1);
		expect(results![0].results).to.have.length(1);
		const result = results![0].results[0];
		expect(result._source).deep.equal(encoded);
		expect(serialize(result.value)).deep.equal(encoded);
		expect(result.value.data).deep.equal(document.data);
		expect(result.context.head).equal(committed.entry.hash);
		expect(result.context.created).equal(created);
		expect(result.context.modified).equal(created);
		expect(await observer.docs.index.getSize()).equal(0);
		expect(observer.docs.log.log.length).equal(0);

		const starts = events.filter(
			(event) => event.name === "documents.query.start",
		);
		expect(starts).to.have.length(1);
		const queryTraceId = starts[0].traceId;
		expect(queryTraceId).match(/^query:/);
		const rpcEvents = events.filter((event) =>
			event.name.startsWith("rpc.request."),
		);
		expect(rpcEvents.length).greaterThan(0);
		expect(
			rpcEvents.some(
				(event) =>
					event.name === "rpc.request.publish" &&
					event.details?.edge === "start",
			),
		).equal(true);
		const responseEvents = rpcEvents.filter(
			(event) => event.name === "rpc.request.response",
		);
		expect(responseEvents).to.have.length(1);
		expect(responseEvents[0].peer).equal(donorHash);
		expect(responseEvents[0].details).include({
			receivedResponses: 1,
			unresolvedResponders: 0,
		});
		const rpcSettlements = rpcEvents.filter(
			(event) => event.name === "rpc.request.settle",
		);
		expect(rpcSettlements).to.have.length(1);
		expect(rpcSettlements[0].details).include({
			outcome: "fulfilled",
			reason: "responses",
			receivedResponses: 1,
			unresolvedResponders: 0,
		});
		const requestTraceIds = new Set(
			rpcEvents.map((event) => event.details?.requestTraceId),
		);
		expect(requestTraceIds.size).equal(1);
		expect([...requestTraceIds][0]).match(/^rpc:/);
		for (const event of events) {
			expect(event.traceId).equal(queryTraceId);
			expect(event.details?.v).equal(1);
		}
		const terminal = events.filter(
			(event) => event.name === "documents.query.settle",
		);
		expect(terminal).to.have.length(1);
		expect(terminal[0].details).include({
			outcome: "fulfilled",
			missingGroups: 0,
		});
		expect(events.at(-1)).equal(terminal[0]);
	});
});
