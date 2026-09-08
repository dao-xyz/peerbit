import { randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { ReplicationIntent, getSamples } from "../src/ranges.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("isLeader maturity routing", () => {
	let session: TestSession | undefined;
	const sandbox = sinon.createSandbox();

	afterEach(async () => {
		sandbox.restore();
		await session?.stop();
		session = undefined;
	});

	const fixture = async (
		ranges: {
			hash: string;
			timestamp: bigint;
			offset?: number;
			width?: number;
			mode?: ReplicationIntent;
		}[],
		filter = new Set(ranges.map((range) => range.hash)),
		strictFullReplicaFallback = true,
	) => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: false,
				domain: createReplicationDomainHash("u32"),
				nativeRangePlanner: false,
				nativeGraph: false,
				timeUntilRoleMaturity: 100,
				strictFullReplicaFallback,
			},
		});
		const log = store.log as any;
		expect(log._nativeRangePlanner).to.equal(undefined);
		expect(log._nativeBackbone).to.equal(undefined);
		for (const range of ranges) {
			await log.replicationIndex.put(
				new log.indexableDomain.constructorRange({
					id: randomBytes(32),
					publicKeyHash: range.hash,
					timestamp: range.timestamp,
					offset: range.offset ?? 0,
					width: range.width ?? log.indexableDomain.numbers.maxValue,
					mode: range.mode,
				}),
			);
		}
		// Fix only the clock, not timers. Seed the peer view explicitly; exercise
		// the real routing shortcut and range sampler, without network timing.
		sandbox.useFakeTimers({ now: 1_000, toFake: ["Date"] });
		sandbox.stub(log, "createLeaderSelectionContext").resolves({
			roleAge: 100,
			selfHash: store.node.identity.publicKey.hashcode(),
			selfReplicating: false,
			peerFilter: filter,
			peerFilterArray: [...filter],
		});
		const sample = (points: number[]) =>
			getSamples(
				points,
				log.replicationIndex,
				100,
				log.indexableDomain.numbers,
				{
					peerFilter: filter,
					uniqueReplicators: filter,
				},
			);
		const route = (points: number[], candidates?: Iterable<string>) =>
			log._findLeadersUncached(points, { freshLeaderPlan: true, candidates });
		return { log, filter, sample, route };
	};

	it("keeps three young intersecting custodians beside one mature owner", async () => {
		const { sample, route } = await fixture([
			{ hash: "mature", timestamp: 0n },
			{ hash: "young-a", timestamp: 950n },
			{ hash: "young-b", timestamp: 950n },
			{ hash: "young-c", timestamp: 950n },
		]);
		const points = [10, 20, 30];
		const expected = await sample(points);
		expect([...expected.keys()]).to.have.members([
			"mature",
			"young-a",
			"young-b",
			"young-c",
		]);
		expect(await route(points)).to.deep.equal(expected);
	});

	for (const includeStrict of [true, false]) {
		for (const intersects of [true, false]) {
			it(`preserves strict fallback policy (${includeStrict}) with young intersection (${intersects})`, async () => {
				const { route } = await fixture(
					[
						{
							hash: "strict",
							timestamp: 0n,
							offset: 10,
							width: 10,
							mode: ReplicationIntent.Strict,
						},
						{
							hash: "young",
							timestamp: 950n,
							offset: intersects ? 40 : 80,
							width: 20,
						},
					],
					undefined,
					includeStrict,
				);
				const expected = new Map<string, { intersecting: boolean }>();
				if (includeStrict) expected.set("strict", { intersecting: true });
				if (intersects) expected.set("young", { intersecting: true });
				expect(await route([50, 75])).to.deep.equal(expected);
			});
		}
	}

	it("keeps explicit candidates on the coordinate-only path", async () => {
		const { route, sample, filter } = await fixture([
			{
				hash: "strict",
				timestamp: 0n,
				offset: 10,
				width: 10,
				mode: ReplicationIntent.Strict,
			},
			{ hash: "young", timestamp: 950n, offset: 40, width: 20 },
		]);
		const expected = await sample([50, 75]);
		expect([...expected.keys()]).to.deep.equal(["young"]);
		expect(await route([50, 75], filter)).to.deep.equal(expected);
	});

	it("does not promote a young nonintersecting range to fallback ownership", async () => {
		const { sample, route } = await fixture([
			{ hash: "mature", timestamp: 0n, offset: 0, width: 40 },
			{ hash: "young", timestamp: 950n, offset: 80, width: 10 },
		]);
		const expected = await sample([10]);
		expect([...expected.keys()]).to.deep.equal(["mature"]);
		expect(await route([10])).to.deep.equal(expected);
	});

	it("preserves fallback ownership flags and order while adding a young coordinate owner", async () => {
		const { route, sample } = await fixture([
			{ hash: "mature", timestamp: 0n, offset: 10, width: 10 },
			{ hash: "young", timestamp: 950n, offset: 40, width: 20 },
		]);
		expect((await sample([50, 75])).get("mature")).to.deep.equal({
			intersecting: false,
		});
		const actual = await route([50, 75]);
		expect([...actual]).to.deep.equal([
			["mature", { intersecting: true }],
			["young", { intersecting: true }],
		]);
	});

	for (const youngFirst of [true, false]) {
		it(`keeps complete mature-owner shortcuts with mixed-age ranges (${youngFirst ? "young first" : "mature first"})`, async () => {
			const young = { hash: "same-owner", timestamp: 950n };
			const mature = { hash: "same-owner", timestamp: 0n };
			const { log, filter, route } = await fixture(
				youngFirst ? [young, mature] : [mature, young],
			);
			const expected = new Map([["same-owner", { intersecting: true }]]);
			expect(await log.findFullReplicaLeaderPlan(1, 100, filter)).to.deep.equal(
				{ leaders: expected, complete: true },
			);
			expect(await route([10])).to.deep.equal(expected);
		});
	}

	it("does not let an excluded young peer disable a complete filtered shortcut", async () => {
		const { log, filter, route } = await fixture(
			[
				{ hash: "mature", timestamp: 0n },
				{ hash: "excluded", timestamp: 950n },
			],
			new Set(["mature"]),
		);
		const expected = new Map([["mature", { intersecting: true }]]);
		expect(await log.findFullReplicaLeaderPlan(1, 100, filter)).to.deep.equal({
			leaders: expected,
			complete: true,
		});
		expect(await route([10])).to.deep.equal(expected);
	});
});
