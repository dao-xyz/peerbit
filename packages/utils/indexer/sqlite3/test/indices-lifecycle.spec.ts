import { field, variant } from "@dao-xyz/borsh";
import { NotStartedError, id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLiteIndex, SQLiteIndices } from "../src/engine.js";
import type { Database, Statement } from "../src/types.js";

@variant("indices-lifecycle-document")
class Document {
	@id({ type: "string" })
	id!: string;

	@field({ type: "string" })
	value!: string;
}

@variant("indices-lifecycle-other-document")
class OtherDocument {
	@id({ type: "string" })
	id!: string;
}

type Gate = {
	entered: Promise<void>;
	release: () => void;
};

type SignalledGate = Gate & {
	markEntered: () => void;
	waitForRelease: Promise<void>;
};

const createSignalledGate = (): SignalledGate => {
	let markEntered!: () => void;
	let release!: () => void;
	const enteredSignal = new Promise<void>((resolve) => {
		markEntered = resolve;
	});
	const released = new Promise<void>((resolve) => {
		release = resolve;
	});
	return {
		entered: enteredSignal,
		markEntered,
		release,
		waitForRelease: released,
	};
};

type LifecycleDatabase = Database & {
	readonly databaseCalls: number;
	readonly closeCalls: number;
	readonly dropCalls: number;
	readonly sqliteMasterCalls: number;
};

const createMockStatement = (
	sid: string,
	assertOpen: () => void,
	beforeAll?: () => Promise<void>,
): Statement => ({
	id: sid,
	bind: async () => createMockStatement(sid, assertOpen, beforeAll),
	run: async () => {
		assertOpen();
	},
	get: async () => {
		assertOpen();
		return undefined;
	},
	all: async () => {
		assertOpen();
		await beforeAll?.();
		assertOpen();
		return [];
	},
	reset: async () => createMockStatement(sid, assertOpen, beforeAll),
});

const createMockDatabase = (options?: {
	initialStatus?: "open" | "closed";
	sqliteMasterGate?: SignalledGate;
	sqliteMasterFailures?: number;
	openGate?: SignalledGate;
	openFailures?: number;
	statusFailures?: number;
	closeGate?: SignalledGate;
}): LifecycleDatabase => {
	let status = options?.initialStatus ?? "open";
	let databaseCalls = 0;
	let closeCalls = 0;
	let dropCalls = 0;
	let sqliteMasterCalls = 0;
	let sqliteMasterFailures = options?.sqliteMasterFailures ?? 0;
	let openFailures = options?.openFailures ?? 0;
	let statusFailures = options?.statusFailures ?? 0;
	const statements = new Map<string, Statement>();
	const assertOpen = () => {
		if (status !== "open") {
			throw new Error("Database not open");
		}
	};
	return {
		get databaseCalls() {
			return databaseCalls;
		},
		get closeCalls() {
			return closeCalls;
		},
		get dropCalls() {
			return dropCalls;
		},
		get sqliteMasterCalls() {
			return sqliteMasterCalls;
		},
		exec: async () => {
			databaseCalls++;
			assertOpen();
		},
		prepare: async (sql: string, key?: string) => {
			databaseCalls++;
			assertOpen();
			const isSQLiteMaster = sql.includes("sqlite_master");
			const statement = createMockStatement(
				key ?? sql,
				assertOpen,
				isSQLiteMaster
					? async () => {
							sqliteMasterCalls++;
							if (options?.sqliteMasterGate) {
								options.sqliteMasterGate.markEntered();
								await options.sqliteMasterGate.waitForRelease;
							}
							if (sqliteMasterFailures > 0) {
								sqliteMasterFailures--;
								throw new Error("Forced sqlite_master failure");
							}
						}
					: undefined,
			);
			if (key) {
				statements.set(key, statement);
			}
			return statement;
		},
		open: async () => {
			databaseCalls++;
			if (options?.openGate) {
				options.openGate.markEntered();
				await options.openGate.waitForRelease;
			}
			if (openFailures > 0) {
				openFailures--;
				throw new Error("Forced open failure");
			}
			status = "open";
		},
		close: async () => {
			closeCalls++;
			if (options?.closeGate) {
				options.closeGate.markEntered();
				await options.closeGate.waitForRelease;
			}
			status = "closed";
		},
		drop: async () => {
			dropCalls++;
			status = "closed";
		},
		status: () => {
			if (statusFailures > 0) {
				statusFailures--;
				throw new Error("Forced status failure");
			}
			return status;
		},
		statements: {
			get: (key: string) => statements.get(key),
			get size() {
				return statements.size;
			},
		},
	};
};

const expectNotStarted = async (promise: unknown) => {
	try {
		await promise;
	} catch (error) {
		expect(error).to.be.instanceOf(NotStartedError);
		return;
	}
	expect.fail("Expected NotStartedError");
};

describe("SQLiteIndices lifecycle admission", () => {
	it("awaits an admitted index initialization before closing the database", async () => {
		const sqliteMasterGate = createSignalledGate();
		const db = createMockDatabase({ sqliteMasterGate });
		const indices = new SQLiteIndices({ db });
		await indices.start();

		const initializing = indices.init({ schema: Document });
		await sqliteMasterGate.entered;
		const stopping = indices.stop();
		await Promise.resolve();
		const closedBeforeInitializationFinished = db.closeCalls;

		sqliteMasterGate.release();
		const [initResult, stopResult] = await Promise.allSettled([
			initializing,
			stopping,
		]);

		expect(closedBeforeInitializationFinished).to.equal(0);
		expect(initResult.status).to.equal("fulfilled");
		expect(stopResult.status).to.equal("fulfilled");
		expect(db.closeCalls).to.equal(1);
		if (initResult.status === "fulfilled") {
			await expectNotStarted(initResult.value.getSize());
		}
	});

	it("registers an admitted scope before shutdown drains children", async () => {
		const db = createMockDatabase();
		const indices = new SQLiteIndices({ db });
		await indices.start();

		const creatingScope = indices.scope("admitted");
		const stopping = indices.stop();
		const [scopeResult, stopResult] = await Promise.allSettled([
			creatingScope,
			stopping,
		]);

		expect(scopeResult.status).to.equal("fulfilled");
		expect(stopResult.status).to.equal("fulfilled");
		if (scopeResult.status !== "fulfilled") return;

		const callsBeforePostStopInit = db.databaseCalls;
		const index = (await scopeResult.value.init({
			schema: Document,
		})) as SQLiteIndex<Document>;
		expect(db.databaseCalls).to.equal(callsBeforePostStopInit);
		expect(index.closed).to.equal(true);
	});

	it("coalesces duplicate same-schema initialization into one started index", async () => {
		const sqliteMasterGate = createSignalledGate();
		const db = createMockDatabase({ sqliteMasterGate });
		const indices = new SQLiteIndices({ db });
		await indices.start();

		const firstInitialization = indices.init({ schema: Document });
		await sqliteMasterGate.entered;
		const duplicateInitialization = indices.init({ schema: Document });
		sqliteMasterGate.release();

		const [first, duplicate] = await Promise.all([
			firstInitialization,
			duplicateInitialization,
		]);
		expect(duplicate).to.equal(first);
		expect((first as SQLiteIndex<Document>).closed).to.equal(false);
		expect(db.sqliteMasterCalls).to.equal(1);
		await indices.stop();
	});

	it("coalesces duplicate same-name scopes into one started scope", async () => {
		const db = createMockDatabase();
		const indices = new SQLiteIndices({ db });
		await indices.start();

		const [first, duplicate] = await Promise.all([
			indices.scope("shared"),
			indices.scope("shared"),
		]);
		expect(duplicate).to.equal(first);

		const [firstIndex, duplicateIndex] = await Promise.all([
			first.init({ schema: Document }),
			duplicate.init({ schema: Document }),
		]);
		expect(duplicateIndex).to.equal(firstIndex);
		expect((firstIndex as SQLiteIndex<Document>).closed).to.equal(false);
		expect(db.sqliteMasterCalls).to.equal(1);
		await indices.stop();
	});

	it("rejects new work while closing before it can access the database", async () => {
		const closeGate = createSignalledGate();
		const db = createMockDatabase({ closeGate });
		const indices = new SQLiteIndices({ db });
		await indices.start();
		const child = await indices.scope("child");

		const stopping = indices.stop();
		await closeGate.entered;
		const callsAtClosing = db.databaseCalls;
		const concurrentStop = indices.stop();

		try {
			await expectNotStarted(indices.init({ schema: Document }));
			await expectNotStarted(indices.scope("too-late"));
			await expectNotStarted(child.init({ schema: OtherDocument }));
			await expectNotStarted(child.scope("too-late-child"));
			await expectNotStarted(indices.drop());
			expect(db.databaseCalls).to.equal(callsAtClosing);
			expect(concurrentStop).to.equal(stopping);
		} finally {
			closeGate.release();
			await Promise.allSettled([stopping, concurrentStop]);
		}
		expect(db.closeCalls).to.equal(1);
		expect(db.dropCalls).to.equal(0);
	});

	it("removes a failed initialization so the same schema can retry", async () => {
		const db = createMockDatabase({ sqliteMasterFailures: 1 });
		const indices = new SQLiteIndices({ db });
		await indices.start();

		const [failed] = await Promise.allSettled([
			indices.init({ schema: Document }),
		]);
		expect(failed.status).to.equal("rejected");
		if (failed.status === "rejected") {
			expect((failed.reason as Error).message).to.equal(
				"Forced sqlite_master failure",
			);
		}

		await indices.stop();
		await indices.start();
		const retried = (await indices.init({
			schema: Document,
		})) as SQLiteIndex<Document>;
		expect(retried.closed).to.equal(false);
		await indices.stop();
	});

	it("waits for an owner restart before returning a cached index", async () => {
		const openGate = createSignalledGate();
		const db = createMockDatabase({ openGate });
		const indices = new SQLiteIndices({ db });
		await indices.start();
		const original = await indices.init({ schema: Document });
		await indices.stop();

		const restarting = indices.start();
		await openGate.entered;
		let reinitSettled = false;
		const reinitializing = indices.init({ schema: Document }).finally(() => {
			reinitSettled = true;
		});
		await Promise.resolve();
		const settledBeforeOpen = reinitSettled;

		openGate.release();
		const [restartResult, reinitResult] = await Promise.allSettled([
			restarting,
			reinitializing,
		]);
		expect(settledBeforeOpen).to.equal(false);
		expect(restartResult.status).to.equal("fulfilled");
		expect(reinitResult.status).to.equal("fulfilled");
		if (reinitResult.status === "fulfilled") {
			expect(reinitResult.value).to.equal(original);
			expect((reinitResult.value as SQLiteIndex<Document>).closed).to.equal(
				false,
			);
		}
		await indices.stop();
	});

	it("binds nested admissions to an in-flight ancestor start", async () => {
		const sqliteMasterGate = createSignalledGate();
		const db = createMockDatabase({ sqliteMasterGate });
		const indices = new SQLiteIndices({ db });
		const blocker = await indices.scope("blocker");
		await blocker.init({ schema: Document });
		const later = await indices.scope("later");

		const starting = indices.start();
		await sqliteMasterGate.entered;
		let initSettled = false;
		let scopeSettled = false;
		const descendantInit = Promise.resolve(
			later.init({ schema: OtherDocument }),
		).finally(() => {
			initSettled = true;
		});
		const descendantScope = Promise.resolve(later.scope("nested")).finally(
			() => {
				scopeSettled = true;
			},
		);
		await Promise.resolve();
		expect(initSettled).to.equal(false);
		expect(scopeSettled).to.equal(false);

		sqliteMasterGate.release();
		const [index, nested] = await Promise.all([
			descendantInit,
			descendantScope,
			starting,
		]);
		expect((index as SQLiteIndex<OtherDocument>).closed).to.equal(false);
		const nestedIndex = (await nested.init({
			schema: Document,
		})) as SQLiteIndex<Document>;
		expect(nestedIndex.closed).to.equal(false);
		await indices.stop();
	});

	it("rejects nested admissions with an ancestor start and allows retry", async () => {
		const sqliteMasterGate = createSignalledGate();
		const db = createMockDatabase({
			sqliteMasterGate,
			sqliteMasterFailures: 1,
		});
		const indices = new SQLiteIndices({ db });
		const blocker = await indices.scope("blocker");
		await blocker.init({ schema: Document });
		const later = await indices.scope("later");

		const starting = indices.start();
		await sqliteMasterGate.entered;
		const descendantInit = later.init({ schema: OtherDocument });
		const descendantScope = later.scope("nested");
		sqliteMasterGate.release();
		const [startResult, initResult, scopeResult] = await Promise.allSettled([
			starting,
			descendantInit,
			descendantScope,
		]);
		expect(startResult.status).to.equal("rejected");
		expect(initResult.status).to.equal("rejected");
		expect(scopeResult.status).to.equal("rejected");
		for (const result of [startResult, initResult, scopeResult]) {
			if (result.status === "rejected") {
				expect((result.reason as Error).message).to.equal(
					"Forced sqlite_master failure",
				);
			}
		}

		await indices.start();
		const retriedIndex = (await later.init({
			schema: OtherDocument,
		})) as SQLiteIndex<OtherDocument>;
		const retriedScope = await later.scope("nested");
		const nestedIndex = (await retriedScope.init({
			schema: Document,
		})) as SQLiteIndex<Document>;
		expect(retriedIndex.closed).to.equal(false);
		expect(nestedIndex.closed).to.equal(false);
		await indices.stop();
	});

	it("binds cached initialization to a synchronously failing owner start", async () => {
		const db = createMockDatabase({
			initialStatus: "closed",
			statusFailures: 1,
		});
		const indices = new SQLiteIndices({ db });
		const original = await indices.init({ schema: Document });

		const restarting = indices.start();
		const reinitializing = indices.init({ schema: Document });
		const [restartResult, reinitResult] = await Promise.allSettled([
			restarting,
			reinitializing,
		]);
		expect(restartResult.status).to.equal("rejected");
		expect(reinitResult.status).to.equal("rejected");
		if (restartResult.status === "rejected") {
			expect((restartResult.reason as Error).message).to.equal(
				"Forced status failure",
			);
		}
		if (reinitResult.status === "rejected") {
			expect((reinitResult.reason as Error).message).to.equal(
				"Forced status failure",
			);
		}

		await indices.start();
		const retried = await indices.init({ schema: Document });
		expect(retried).to.equal(original);
		expect((retried as SQLiteIndex<Document>).closed).to.equal(false);
		await indices.stop();
	});

	it("keeps a cached schema retryable after an owner restart fails", async () => {
		const db = createMockDatabase({ openFailures: 1 });
		const indices = new SQLiteIndices({ db });
		await indices.start();
		const original = await indices.init({ schema: Document });
		await indices.stop();

		const [failedRestart, failedInit] = await Promise.allSettled([
			indices.start(),
			indices.init({ schema: Document }),
		]);
		expect(failedRestart.status).to.equal("rejected");
		expect(failedInit.status).to.equal("rejected");

		await indices.start();
		const retried = await indices.init({ schema: Document });
		expect(retried).to.equal(original);
		expect((retried as SQLiteIndex<Document>).closed).to.equal(false);
		await indices.stop();
	});

	it("removes a scope whose admitted activation fails so it can retry", async () => {
		const db = createMockDatabase({
			initialStatus: "closed",
			openFailures: 1,
		});
		const indices = new SQLiteIndices({ db });

		const starting = indices.start();
		const activatingScope = indices.scope("retry");
		const [startResult, scopeResult] = await Promise.allSettled([
			starting,
			activatingScope,
		]);
		expect(startResult.status).to.equal("rejected");
		expect(scopeResult.status).to.equal("rejected");

		await indices.start();
		const retriedScope = await indices.scope("retry");
		const index = (await retriedScope.init({
			schema: Document,
		})) as SQLiteIndex<Document>;
		expect(index.closed).to.equal(false);
		await indices.stop();
	});

	it("can restart after one child stop fails without poisoning its sibling", async () => {
		const db = createMockDatabase();
		const indices = new SQLiteIndices({ db });
		await indices.start();
		const failing = await indices.scope("failing");
		const sibling = await indices.scope("sibling");
		const failingIndex = await failing.init({ schema: Document });
		const originalStop = failingIndex.stop.bind(failingIndex);
		let stopAttempts = 0;
		failingIndex.stop = async () => {
			stopAttempts++;
			if (stopAttempts === 1) {
				throw new Error("Forced child index stop failure");
			}
			await originalStop();
		};

		const [failedStop] = await Promise.allSettled([indices.stop()]);
		expect(failedStop.status).to.equal("rejected");
		if (failedStop.status === "rejected") {
			expect((failedStop.reason as Error).message).to.equal(
				"Forced child index stop failure",
			);
		}

		await indices.start();
		const siblingIndex = (await sibling.init({
			schema: OtherDocument,
		})) as SQLiteIndex<OtherDocument>;
		expect(siblingIndex.closed).to.equal(false);
		expect(stopAttempts).to.equal(1);
		await indices.stop();
	});

	it("keeps pre-start and completed-stop setup lazy until the next start", async () => {
		const db = createMockDatabase({ initialStatus: "closed" });
		const indices = new SQLiteIndices({ db });
		const callsBeforeSetup = db.databaseCalls;

		const preStartScope = await indices.scope("pre-start");
		const preStartIndex = (await preStartScope.init({
			schema: Document,
		})) as SQLiteIndex<Document>;
		expect(db.databaseCalls).to.equal(callsBeforeSetup);
		expect(preStartIndex.closed).to.equal(true);

		await indices.start();
		expect(preStartIndex.closed).to.equal(false);
		await indices.stop();

		const callsAfterStop = db.databaseCalls;
		const postStopIndex = (await indices.init({
			schema: OtherDocument,
		})) as SQLiteIndex<OtherDocument>;
		const postStopScope = await indices.scope("post-stop");
		expect(db.databaseCalls).to.equal(callsAfterStop);
		expect(postStopIndex.closed).to.equal(true);

		await indices.start();
		expect(postStopIndex.closed).to.equal(false);
		expect(postStopScope).to.be.instanceOf(SQLiteIndices);
		await indices.stop();
	});
});
