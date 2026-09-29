import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { type Args, SharedLog } from "../src/index.js";
import { JSON_ENCODING } from "./utils/stores/encoding.js";

const options: Args<string, any, any> = {
	encoding: JSON_ENCODING,
	replicate: { factor: 1 },
	nativeGraph: false,
	nativeBackbone: false,
	nativeRangePlanner: false,
	timeUntilRoleMaturity: 0,
};

const trusted = (log: SharedLog<string, any, any>) =>
	log as unknown as {
		openWithLocalRecovery(
			args: Args<string, any, any>,
			recover: () => Promise<void | "local-only">,
		): Promise<void>;
	};

@variant("shared-log-local-recovery-open-test")
class RecoveryOwner extends Program {
	@field({ type: SharedLog })
	log: SharedLog<string, any, any>;

	recover?: () => Promise<void>;

	constructor() {
		super();
		this.log = new SharedLog();
	}

	async open() {
		if (this.recover) {
			await trusted(this.log).openWithLocalRecovery(options, this.recover);
		} else {
			await this.log.open(options);
		}
	}
}

describe("shared-log local recovery before communication", function () {
	this.timeout(30_000);
	let session: TestSession;

	beforeEach(async () => {
		session = await TestSession.connected(1);
	});

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
	});

	const holdRecovery = (owner: RecoveryOwner, failure?: Error) => {
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const rpcOpen = sinon.spy(owner.log.rpc, "open");
		const subscribe = sinon.spy(session.peers[0].services.pubsub, "subscribe");
		owner.recover = async () => {
			expect(owner.log.log.closed).equal(false);
			expect(owner.log.syncronizer).not.equal(undefined);
			entered.resolve();
			await release.promise;
			if (failure) throw failure;
		};
		const outcome = session.peers[0].open(owner).then(
			(value) => ({ value, error: undefined }),
			(error: unknown) => ({ value: undefined, error }),
		);
		return {
			entered: entered.promise,
			release: release.resolve,
			outcome,
			assertNotServing() {
				expect(rpcOpen.called).equal(false);
				expect(subscribe.calledWith(owner.log.topic)).equal(false);
				expect((owner.log.rpc as any)._listenerAttached === true).equal(false);
				expect((owner.log.rpc as any)._subscribed === true).equal(false);
			},
			assertServing() {
				expect(rpcOpen.calledOnce).equal(true);
				expect(subscribe.calledWith(owner.log.topic)).equal(true);
				expect((owner.log.rpc as any)._listenerAttached).equal(true);
				expect((owner.log.rpc as any)._subscribed).equal(true);
			},
		};
	};

	it("waits for local recovery, rejects overlapping opens, and preserves virtual open", async () => {
		const owner = new RecoveryOwner();
		const original = owner.log.open.bind(owner.log);
		const override = sinon
			.stub(owner.log, "open")
			.callsFake((args) => original({ ...args, keep: () => true }));
		const held = holdRecovery(owner);
		try {
			await held.entered;
			held.assertNotServing();
			expect(override.calledOnce).equal(true);
			await expect(owner.log.open(options)).rejectedWith("already in progress");
			await expect(
				trusted(owner.log).openWithLocalRecovery(options, async () => {}),
			).rejectedWith("idle closed log");
			held.assertNotServing();
			held.release();
			const result = await held.outcome;
			if (result.error) throw result.error;
			expect(result.value).equal(owner);
			held.assertServing();
			await owner.log.append("after recovery", { target: "none" });
			expect(owner.log.log.length).equal(1);
		} finally {
			held.release();
			await held.outcome;
		}
	});

	it("does not advertise failed recovery and resets the seam for ordinary reopen", async () => {
		const owner = new RecoveryOwner();
		const failure = new Error("injected recovery failure");
		const held = holdRecovery(owner, failure);
		try {
			await held.entered;
			held.assertNotServing();
			held.release();
			expect((await held.outcome).error).equal(failure);
			held.assertNotServing();
		} finally {
			held.release();
			await held.outcome;
		}
		await owner.close();
		owner.recover = undefined;
		await session.peers[0].open(owner);
		held.assertServing();
		await owner.log.append("ordinary reopen", { target: "none" });
		expect(owner.log.log.length).equal(1);
	});

	it("does not activate communication if the lower log closes during recovery", async () => {
		const owner = new RecoveryOwner();
		const held = holdRecovery(owner);
		try {
			await held.entered;
			// ProgramHandler already rejects closing an owner whose open is in
			// flight. Also fail closed if its local log is closed directly.
			await owner.log.log.close();
			held.release();
			expect((await held.outcome).error).instanceOf(Error);
			expect(((await held.outcome).error as Error).message).equal(
				"SharedLog closed during local recovery",
			);
			held.assertNotServing();
		} finally {
			held.release();
			await held.outcome;
		}
	});

	it("rejects an override that skips the mandatory recovery callback", async () => {
		const owner = new RecoveryOwner();
		const recovered = sinon.stub().resolves();
		owner.recover = recovered;
		sinon.stub(owner.log, "open").resolves();
		await expect(session.peers[0].open(owner)).rejectedWith(
			"did not complete local recovery",
		);
		expect(recovered.called).equal(false);
	});

	it("recovers a frozen log locally without communication and permits later normal reopen", async () => {
		const log = new SharedLog<string, any, any>();
		await log.beforeOpen(session.peers[0]);
		const rpcOpen = sinon.spy(log.rpc, "open");
		const subscribe = sinon.spy(session.peers[0].services.pubsub, "subscribe");
		try {
			await trusted(log).openWithLocalRecovery(options, async () => {
				expect(log.log.closed).equal(false);
				expect(log.syncronizer).not.equal(undefined);
				return "local-only";
			});
			expect(rpcOpen.called).equal(false);
			expect(subscribe.calledWith(log.topic)).equal(false);
			expect((log.rpc as any)._listenerAttached === true).equal(false);
		} finally {
			await log.close();
		}
		await session.peers[0].open(log, { args: options });
		expect(rpcOpen.calledOnce).equal(true);
		expect(subscribe.calledWith(log.topic)).equal(true);
		await log.append("normal reopen after local recovery", { target: "none" });
		expect(log.log.length).equal(1);
	});
});
