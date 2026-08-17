import { toHexString } from "@peerbit/crypto";
import { expect } from "chai";
import {
	mkdir,
	mkdtemp,
	readdir,
	rm,
	symlink,
	writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
	type CustodyRecordPersistence,
	CustodyRecordStore,
} from "../src/custody-store.js";
import { type RebalanceScanPlan, ReplicationIntent } from "../src/ranges.js";
import {
	RebalanceWorkMemoryPersistence,
	type RebalanceWorkNodeModules,
	openMemoryRebalanceWorkStore,
	openNodeRebalanceWorkStore,
} from "../src/rebalance-work-persistence.js";
import {
	DEFAULT_REBALANCE_WORK_LIMITS,
	REBALANCE_WORK_FILES,
	type RebalanceWorkPersistence,
} from "../src/rebalance-work-store.js";

const viewId = (character: string) => character.repeat(64);

const makePlan = (offset = 10): RebalanceScanPlan<"u32"> => ({
	boundary: false,
	geometryRanges: [
		{
			start1: offset,
			end1: offset + 10,
			start2: offset,
			end2: offset + 10,
			mode: ReplicationIntent.Strict,
		},
	],
	ownedIntervals: [
		{ start: BigInt(offset), end: BigInt(offset + 10), geometryTask: 0 },
	],
	taskCount: 1,
	historyMutations: [{ rangeHash: `range-${offset}`, present: true }],
});

const install = async (
	store: Awaited<ReturnType<typeof openNodeRebalanceWorkStore>>,
	character = "a",
) =>
	store.install(store.snapshot().revision, {
		resolution: "u32",
		viewId: viewId(character),
		plan: makePlan(),
	});

const fakeNodeModules = (
	options: {
		missingBarrier?: boolean;
		rawCloseError?: Error;
		leaseCloseError?: Error;
	} = {},
) => {
	const events: string[] = [];
	const directories = new Set(["/node"]);
	const files = new Map<string, Uint8Array>();
	let nextSyncFailure:
		| Readonly<{ directory: string; error: Error }>
		| undefined;

	const fs = {
		async realpath(path: string) {
			return path;
		},
		async stat(path: string) {
			return { isDirectory: () => directories.has(path) };
		},
		async mkdir(path: string) {
			if (directories.has(path)) {
				throw Object.assign(new Error("exists"), { code: "EEXIST" });
			}
			directories.add(path);
		},
		async open(path: string, _flags: "r") {
			return {
				async sync() {
					events.push(`fs:sync:${path}`);
					if (nextSyncFailure?.directory === path) {
						const failure = nextSyncFailure.error;
						nextSyncFailure = undefined;
						throw failure;
					}
				},
				async close() {
					events.push(`fs:close:${path}`);
				},
			};
		},
	};

	class RawStore {
		readonly durableBarrier = options.missingBarrier
			? undefined
			: async (name?: string) => {
					events.push(`raw:barrier:${name}`);
				};

		async read(name: string, maxBytes?: number) {
			events.push(`raw:read:${name}:${maxBytes}`);
			const bytes = files.get(name);
			return bytes ? new Uint8Array(bytes) : undefined;
		}

		async write(name: string, bytes: Uint8Array) {
			events.push(`raw:write:${name}`);
			files.set(name, new Uint8Array(bytes));
		}

		async append() {}

		async flush(name?: string) {
			events.push(`raw:flush:${name}`);
		}

		async close() {
			events.push("raw:close");
			if (options.rawCloseError) throw options.rawCloseError;
		}
	}

	const lease = {
		fence: Object.freeze({ epoch: 1n, ownerId: "owner", domainId: "domain" }),
		async assertHeld() {},
		async runWhileHeld<T>(operation: () => Promise<T>): Promise<T> {
			return operation();
		},
		async close() {
			events.push("lease:close");
			if (options.leaseCloseError) throw options.leaseCloseError;
		},
	};

	const modules = {
		fs,
		path: { join, dirname },
		native: {
			NativeBackboneNodeCoordinatePersistenceStore: RawStore,
			async acquireNativeDurabilityNodeLease(directory: string) {
				events.push(`lease:acquire:${directory}`);
				return lease;
			},
		},
	} as RebalanceWorkNodeModules;

	return {
		events,
		files,
		namespace: "/node/rebalance-work/01",
		dependencies: {
			async loadNodeModules() {
				return modules;
			},
		},
		failNextSync(directory: string, error: Error) {
			nextSyncFailure = { directory, error };
		},
	};
};

describe("rebalance work store persistence", function () {
	this.timeout(30_000);

	const directories = new Set<string>();

	const temporaryDirectory = async (prefix: string): Promise<string> => {
		const directory = await mkdtemp(join(tmpdir(), prefix));
		directories.add(directory);
		return directory;
	};

	afterEach(async () => {
		for (const directory of directories) {
			await rm(directory, { recursive: true, force: true });
		}
		directories.clear();
	});

	it("bounds and defensively copies memory persistence", async () => {
		const files = new Map<string, Uint8Array>();
		const persistence = new RebalanceWorkMemoryPersistence(files);
		const input = new Uint8Array([1, 2, 3]);
		await persistence.write(REBALANCE_WORK_FILES[0], input);
		input[0] = 9;

		const first = await persistence.read(REBALANCE_WORK_FILES[0], 3);
		expect([...first!]).to.deep.equal([1, 2, 3]);
		first![1] = 9;
		expect([
			...(await persistence.read(REBALANCE_WORK_FILES[0], 3))!,
		]).to.deep.equal([1, 2, 3]);

		await expect(
			persistence.read(REBALANCE_WORK_FILES[0], 2),
		).to.be.rejectedWith("exceeds the 2 byte read limit");
		await expect(persistence.read("other", 3)).to.be.rejectedWith(
			"Invalid rebalance work persistence file",
		);
		await expect(
			persistence.write("other", new Uint8Array()),
		).to.be.rejectedWith("Invalid rebalance work persistence file");
		expect(() => new RebalanceWorkMemoryPersistence(files)).to.throw(
			"already open",
		);

		await persistence.close();
		await expect(
			persistence.read(REBALANCE_WORK_FILES[0], 3),
		).to.be.rejectedWith("closed");
		const reopened = new RebalanceWorkMemoryPersistence(files);
		await reopened.close();
	});

	it("reopens memory work from the same bounded namespace", async () => {
		const files = new Map<string, Uint8Array>();
		const first = await openMemoryRebalanceWorkStore({ files });
		const installed = await first.install(0n, {
			resolution: "u32",
			viewId: viewId("b"),
			plan: makePlan(),
		});
		await first.close();

		const reopened = await openMemoryRebalanceWorkStore({ files });
		expect(reopened.snapshot()).to.deep.equal(installed.snapshot);
		expect(reopened.currentDurableCommit()).to.equal(undefined);
		await reopened.close();
	});

	it("persists strict work under the canonical per-log namespace", async () => {
		const directory = await temporaryDirectory("peerbit-rebalance-work-");
		const logId = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
		const namespace = join(directory, "rebalance-work", toHexString(logId));

		const first = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId,
		});
		const installed = await install(first);
		expect(installed.durableCommit).to.exist;
		await first.close();

		expect(await readdir(namespace)).to.include.members([
			...REBALANCE_WORK_FILES,
			".peerbit-native-durability-lease",
		]);
		const reopened = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId,
		});
		expect(reopened.snapshot()).to.deep.equal(installed.snapshot);
		expect(reopened.currentDurableCommit()).to.deep.include({ revision: 2n });
		await reopened.close();
	});

	it("holds one canonical namespace lease and isolates log ids", async () => {
		const directory = await temporaryDirectory("peerbit-rebalance-lock-");
		const alias = `${directory}-alias`;
		directories.add(alias);
		await symlink(directory, alias, "dir");
		const logId = new Uint8Array([1, 2, 3]);
		const otherLogId = new Uint8Array([1, 2, 4]);

		const first = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId,
		});
		await expect(
			openNodeRebalanceWorkStore({ nodeDirectory: alias, logId }),
		).to.be.rejectedWith("already open");

		const other = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId: otherLogId,
		});
		await other.close();
		await first.close();

		const reopened = await openNodeRebalanceWorkStore({
			nodeDirectory: alias,
			logId,
		});
		await reopened.close();
	});

	it("releases the namespace lease after failed recovery", async () => {
		const directory = await temporaryDirectory("peerbit-rebalance-invalid-");
		const logId = new Uint8Array([7, 8, 9]);
		const namespace = join(directory, "rebalance-work", toHexString(logId));
		await mkdir(namespace, { recursive: true });
		await writeFile(join(namespace, REBALANCE_WORK_FILES[0]), "invalid");

		await expect(
			openNodeRebalanceWorkStore({ nodeDirectory: directory, logId }),
		).to.be.rejectedWith("No valid rebalance work generation remains");
		await rm(join(namespace, REBALANCE_WORK_FILES[0]));

		const recovered = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId,
		});
		await recovered.close();
	});

	it("fails closed when either physical slot exceeds the read cap", async () => {
		const directory = await temporaryDirectory("peerbit-rebalance-cap-");
		const logId = new Uint8Array([4, 5, 6]);
		const namespace = join(directory, "rebalance-work", toHexString(logId));
		const first = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId,
		});
		const installed = await install(first, "c");
		await first.close();

		// The first install writes the cleared baseline to B and the active frame
		// to A. An unread B could instead hide newer pending custody work.
		await writeFile(
			join(namespace, REBALANCE_WORK_FILES[1]),
			new Uint8Array(DEFAULT_REBALANCE_WORK_LIMITS.maxFrameBytes + 1),
		);
		await expect(
			openNodeRebalanceWorkStore({
				nodeDirectory: directory,
				logId,
			}),
		).to.be.rejectedWith("Failed to read every rebalance work generation");
		expect(installed.snapshot.active).to.not.equal(undefined);
	});

	it("rejects limit widening before it creates or leases a namespace", async () => {
		const directory = await temporaryDirectory("peerbit-rebalance-limit-");
		const logId = new Uint8Array([9]);
		await expect(
			openNodeRebalanceWorkStore({
				nodeDirectory: directory,
				logId,
				limits: {
					maxFrameBytes: DEFAULT_REBALANCE_WORK_LIMITS.maxFrameBytes + 1,
				},
			}),
		).to.be.rejectedWith("maxFrameBytes");
		expect(await readdir(directory)).to.deep.equal([]);

		const opened = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId,
		});
		await opened.close();
	});

	it("rejects a custody authority for another log before filesystem setup", async () => {
		const directory = await temporaryDirectory("peerbit-rebalance-wrong-log-");
		const custodyPersistence: CustodyRecordPersistence = {
			read: async () => undefined,
			write: async () => undefined,
			durableBarrier: async () => undefined,
		};
		const custody = await CustodyRecordStore.open({
			persistence: custodyPersistence,
			durability: "strict",
			binding: {
				logId: new Uint8Array([1]),
				localPublicKey: new Uint8Array([2]),
				role: "source",
			},
		});
		const authority = custody.sourceReceiptAuthority();
		const before = await readdir(directory);
		await expect(
			openNodeRebalanceWorkStore({
				nodeDirectory: directory,
				logId: new Uint8Array([3]),
				custody: {
					sourceReceiptAuthority: authority,
					logId: new Uint8Array([3]),
				},
			}),
		).to.be.rejectedWith("authority log mismatch");
		expect(await readdir(directory)).to.deep.equal(before);
		await custody.close();
	});

	it("rejects a same-parent namespace symlink instead of crossing log ids", async () => {
		const directory = await temporaryDirectory("peerbit-rebalance-alias-");
		const firstId = new Uint8Array([10]);
		const secondId = new Uint8Array([11]);
		const workRoot = join(directory, "rebalance-work");
		const firstNamespace = join(workRoot, toHexString(firstId));
		const secondNamespace = join(workRoot, toHexString(secondId));

		const first = await openNodeRebalanceWorkStore({
			nodeDirectory: directory,
			logId: firstId,
		});
		await install(first, "d");
		await first.close();
		await symlink(firstNamespace, secondNamespace, "dir");

		await expect(
			openNodeRebalanceWorkStore({
				nodeDirectory: directory,
				logId: secondId,
			}),
		).to.be.rejectedWith("not its canonical requested path");
	});

	it("strictly syncs the namespace after each raw file barrier", async () => {
		const fake = fakeNodeModules();
		const store = await openNodeRebalanceWorkStore(
			{ nodeDirectory: "/node", logId: new Uint8Array([1]) },
			fake.dependencies,
		);
		fake.events.length = 0;
		await install(store, "e");

		const firstWrite = fake.events.indexOf(
			`raw:write:${REBALANCE_WORK_FILES[1]}`,
		);
		const firstBarrier = fake.events.indexOf(
			`raw:barrier:${REBALANCE_WORK_FILES[1]}`,
		);
		const firstDirectorySync = fake.events.indexOf(`fs:sync:${fake.namespace}`);
		expect(firstWrite).to.be.at.least(0);
		expect(firstBarrier).to.be.greaterThan(firstWrite);
		expect(firstDirectorySync).to.be.greaterThan(firstBarrier);
		await store.close();
		expect(fake.events.indexOf("raw:close")).to.be.lessThan(
			fake.events.indexOf("lease:close"),
		);
	});

	it("fails closed when the strict namespace sync fails after a file barrier", async () => {
		const fake = fakeNodeModules();
		const store = await openNodeRebalanceWorkStore(
			{ nodeDirectory: "/node", logId: new Uint8Array([1]) },
			fake.dependencies,
		);
		fake.events.length = 0;
		fake.failNextSync(fake.namespace, new Error("namespace sync failed"));

		await expect(install(store, "f")).to.be.rejectedWith(
			"Failed to persist rebalance work frame",
		);
		expect(fake.events).to.include(`raw:barrier:${REBALANCE_WORK_FILES[1]}`);
		expect(fake.events).to.include(`fs:sync:${fake.namespace}`);
		await expect(store.close()).to.be.rejectedWith(
			"Rebalance work store is poisoned",
		);
	});

	it("fences direct Node adapter operations and validates flush names", async () => {
		const fake = fakeNodeModules();
		let persistence: RebalanceWorkPersistence | undefined;
		const store = await openNodeRebalanceWorkStore(
			{ nodeDirectory: "/node", logId: new Uint8Array([1]) },
			{
				...fake.dependencies,
				onPersistenceCreated(value) {
					persistence = value;
				},
			},
		);
		await persistence!.flush?.(REBALANCE_WORK_FILES[0]);
		expect(fake.events).to.include(`raw:flush:${REBALANCE_WORK_FILES[0]}`);
		expect(() => persistence!.flush?.("other")).to.throw(
			"Invalid rebalance work persistence file",
		);
		await store.close();
		expect(() =>
			persistence!.write(REBALANCE_WORK_FILES[0], new Uint8Array()),
		).to.throw("closing");
		expect(() => persistence!.flush?.(REBALANCE_WORK_FILES[0])).to.throw(
			"closing",
		);
	});

	it("closes raw storage before its lease and aggregates both failures", async () => {
		const rawError = new Error("raw close failed");
		const leaseError = new Error("lease close failed");
		const fake = fakeNodeModules({
			rawCloseError: rawError,
			leaseCloseError: leaseError,
		});
		const store = await openNodeRebalanceWorkStore(
			{ nodeDirectory: "/node", logId: new Uint8Array([1]) },
			fake.dependencies,
		);

		let closeError: unknown;
		try {
			await store.close();
		} catch (error) {
			closeError = error;
		}
		expect(closeError).to.be.instanceOf(AggregateError);
		expect((closeError as AggregateError).errors).to.deep.equal([
			rawError,
			leaseError,
		]);
		expect(fake.events.indexOf("raw:close")).to.be.lessThan(
			fake.events.indexOf("lease:close"),
		);
	});

	it("closes raw storage and its lease after post-acquisition setup failure", async () => {
		const fake = fakeNodeModules({ missingBarrier: true });
		await expect(
			openNodeRebalanceWorkStore(
				{ nodeDirectory: "/node", logId: new Uint8Array([1]) },
				fake.dependencies,
			),
		).to.be.rejectedWith("does not expose a physical durability barrier");
		expect(fake.events.indexOf("raw:close")).to.be.greaterThan(
			fake.events.findIndex((event) => event.startsWith("lease:acquire:")),
		);
		expect(fake.events.indexOf("raw:close")).to.be.lessThan(
			fake.events.indexOf("lease:close"),
		);
	});
});
