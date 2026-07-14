import { Timestamp } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { ReplicationIntent } from "../src/ranges.js";
import {
	type ReplicationDomainTime,
	createReplicationDomainTime,
} from "../src/replication-domain-time.js";
import { AbsoluteReplicas } from "../src/replication.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("strict live-range leader routing", () => {
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("samples an intersecting strict viewer when strict full-replica fallback is disabled", async () => {
		const domain = createReplicationDomainTime({
			origin: new Date(0),
			unit: "nanoseconds",
		});
		const replicas = {
			min: new AbsoluteReplicas(2),
			max: new AbsoluteReplicas(2),
		};
		const writer = await session.peers[0].open(
			new EventStore<string, ReplicationDomainTime>(),
			{
				args: {
					domain,
					nativeRangePlanner: false,
					replicas,
					replicate: { factor: 1 },
					strictFullReplicaFallback: false,
					timeUntilRoleMaturity: 0,
				},
			},
		);
		const viewer = await session.peers[1].open(writer.clone(), {
			args: {
				domain,
				nativeRangePlanner: false,
				replicas,
				replicate: {
					factor: 10,
					normalized: false,
					offset: 0,
					strict: true,
				},
				strictFullReplicaFallback: false,
				timeUntilRoleMaturity: 0,
			},
		});

		await waitForResolved(async () =>
			expect(await writer.log.replicationIndex.count()).to.equal(2),
		);

		const viewerHash = viewer.node.identity.publicKey.hashcode();
		const viewerRanges = await writer.log.replicationIndex
			.iterate({ query: { hash: viewerHash } })
			.all();
		expect(viewerRanges).to.have.length(1);
		expect(viewerRanges[0].value.mode).to.equal(ReplicationIntent.Strict);
		expect(viewerRanges[0].value.contains(5)).to.equal(true);

		const { entry } = await writer.add("frame", {
			meta: {
				next: [],
				timestamp: new Timestamp({ wallTime: 5n }),
			},
			replicas: new AbsoluteReplicas(2),
			target: "none",
		});
		const leaders = await writer.log.findLeadersFromEntry(entry, 2, {
			roleAge: 0,
		});

		expect([...leaders.keys()]).to.have.members([
			writer.node.identity.publicKey.hashcode(),
			viewerHash,
		]);
	});
});
