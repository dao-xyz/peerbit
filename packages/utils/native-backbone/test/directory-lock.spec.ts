import { expect } from "chai";
import { ClassicLevel } from "classic-level";
import { type ChildProcess, fork } from "node:child_process";
import { once } from "node:events";
import {
	mkdir,
	mkdtemp,
	open as openFile,
	realpath,
	rm,
	symlink,
} from "node:fs/promises";
import { createRequire, syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
	type NativeDurabilityLease,
	NativeDurabilityLeaseStateError,
	NativeDurabilityLeaseUnavailableError,
	type NativeDurabilityLock,
	NativeDurabilityLockClosedError,
	NativeDurabilityLockDirectorySyncError,
	NativeDurabilityLockStateError,
	NativeDurabilityLockUnavailableError,
} from "../src/durability/lease.js";
import {
	NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
	acquireNativeDurabilityNodeLease,
	acquireNativeDurabilityNodeLock,
} from "../src/durability/node-lease.js";

type WorkerMessage =
	| { event: "held" }
	| { event: "error"; name?: string; code?: string; message: string };

const workerPath = fileURLToPath(
	new URL("./fixtures/directory-lock-worker.js", import.meta.url),
);

const waitForWorkerMessage = async (
	child: ChildProcess,
): Promise<WorkerMessage> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			new Promise<WorkerMessage>((resolve, reject) => {
				child.once("message", (message) => resolve(message as WorkerMessage));
				child.once("error", reject);
				child.once("exit", (code, signal) => {
					reject(
						new Error(
							`Lock worker exited before replying (code=${String(code)}, signal=${String(signal)})`,
						),
					);
				});
			}),
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error("Timed out waiting for lock worker")),
					10_000,
				);
			}),
		]);
	} finally {
		if (timer) {
			clearTimeout(timer);
		}
	}
};

const rejectionOf = async (promise: Promise<unknown>): Promise<unknown> => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("Expected promise to reject");
};

describe("native durability directory lock", () => {
	const directories: string[] = [];
	const children = new Set<ChildProcess>();
	const locks = new Set<NativeDurabilityLock>();
	const leases = new Set<NativeDurabilityLease>();

	beforeEach(function () {
		if (process.platform === "win32") {
			this.skip();
		}
	});

	afterEach(async () => {
		const childExits = [...children]
			.filter((child) => child.exitCode === null && child.signalCode === null)
			.map((child) => {
				const exited = once(child, "exit");
				child.kill("SIGKILL");
				return exited;
			});
		await Promise.allSettled([...locks].map((lock) => lock.close()));
		await Promise.allSettled([...leases].map((lease) => lease.close()));
		await Promise.allSettled(childExits);
		children.clear();
		locks.clear();
		leases.clear();
		await Promise.all(
			directories
				.splice(0)
				.map((directory) => rm(directory, { recursive: true, force: true })),
		);
	});

	const temporaryDirectory = async (): Promise<string> => {
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-durability-lock-"),
		);
		directories.push(directory);
		return directory;
	};

	it("excludes a canonical-directory symlink alias", async () => {
		const root = await temporaryDirectory();
		const directory = join(root, "program");
		const alias = join(root, "program-alias");
		await mkdir(directory);
		await symlink(directory, alias, "dir");

		const first = await acquireNativeDurabilityNodeLock(directory);
		locks.add(first);
		const aliasError = await rejectionOf(
			acquireNativeDurabilityNodeLock(alias),
		);
		expect(aliasError).to.be.instanceOf(NativeDurabilityLockUnavailableError);
		expect(
			(aliasError as NativeDurabilityLockUnavailableError).directory,
		).to.equal(await realpath(directory));
		expect(
			await rejectionOf(acquireNativeDurabilityNodeLease(alias)),
		).to.be.instanceOf(NativeDurabilityLeaseUnavailableError);

		await first.close();
		locks.delete(first);
		const reopened = await acquireNativeDurabilityNodeLock(alias);
		locks.add(reopened);
	});

	it("rejects a symlinked lock child and releases its redirected database", async () => {
		const root = await temporaryDirectory();
		const directory = join(root, "program");
		const redirectedDirectory = join(root, "redirected-lock");
		const lockDirectory = join(
			directory,
			NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
		);
		await mkdir(directory);
		await mkdir(redirectedDirectory);
		await symlink(redirectedDirectory, lockDirectory, "dir");
		const canonicalLockDirectory = join(
			await realpath(directory),
			NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
		);

		const acquisitionError = await rejectionOf(
			acquireNativeDurabilityNodeLock(directory),
		);
		expect(acquisitionError).to.be.instanceOf(NativeDurabilityLockStateError);
		expect(
			(acquisitionError as NativeDurabilityLockStateError).directory,
		).to.equal(canonicalLockDirectory);

		const redirected = new ClassicLevel(redirectedDirectory, {
			keyEncoding: "utf8",
			valueEncoding: "utf8",
		});
		await redirected.open();
		await redirected.close();
		await rm(lockDirectory);
		const recovered = await acquireNativeDurabilityNodeLock(directory);
		locks.add(recovered);
	});

	it("fences close admission synchronously and drains admitted work", async () => {
		const directory = await temporaryDirectory();
		const lock = await acquireNativeDurabilityNodeLock(directory);
		locks.add(lock);
		let releaseOperation!: () => void;
		const operationGate = new Promise<void>((resolve) => {
			releaseOperation = resolve;
		});
		const operation = lock.runWhileHeld(async () => {
			await operationGate;
			return 7;
		});

		const closing = lock.close();
		expect(lock.close()).to.equal(closing);
		let lateOperationEntered = false;
		const lateAssertError = rejectionOf(lock.assertHeld());
		const lateOperationError = rejectionOf(
			lock.runWhileHeld(async () => {
				lateOperationEntered = true;
			}),
		);
		const lockError = await rejectionOf(
			acquireNativeDurabilityNodeLock(directory),
		);
		const leaseError = await rejectionOf(
			acquireNativeDurabilityNodeLease(directory),
		);
		expect(lockError).to.be.instanceOf(NativeDurabilityLockUnavailableError);
		expect(leaseError).to.be.instanceOf(NativeDurabilityLeaseUnavailableError);
		expect(await lateAssertError).to.be.instanceOf(
			NativeDurabilityLockClosedError,
		);
		expect(await lateOperationError).to.be.instanceOf(
			NativeDurabilityLockClosedError,
		);
		expect(lateOperationEntered).to.equal(false);

		releaseOperation();
		expect(await operation).to.equal(7);
		await closing;
		locks.delete(lock);
		const reopened = await acquireNativeDurabilityNodeLock(directory);
		locks.add(reopened);
	});

	it("shares exclusion with fenced leases without advancing their state", async () => {
		const directory = await temporaryDirectory();
		const firstLease = await acquireNativeDurabilityNodeLease(directory);
		leases.add(firstLease);
		const lockWhileLeased = await rejectionOf(
			acquireNativeDurabilityNodeLock(directory),
		);
		expect(lockWhileLeased).to.be.instanceOf(
			NativeDurabilityLockUnavailableError,
		);
		await firstLease.close();
		leases.delete(firstLease);

		const lock = await acquireNativeDurabilityNodeLock(directory);
		locks.add(lock);
		const leaseWhileLocked = await rejectionOf(
			acquireNativeDurabilityNodeLease(directory),
		);
		expect(leaseWhileLocked).to.be.instanceOf(
			NativeDurabilityLeaseUnavailableError,
		);
		await lock.close();
		locks.delete(lock);

		const secondLease = await acquireNativeDurabilityNodeLease(directory);
		leases.add(secondLease);
		expect(secondLease.fence.epoch).to.equal(firstLease.fence.epoch + 1n);
		expect(secondLease.fence.domainId).to.equal(firstLease.fence.domainId);
	});

	it("neither reads nor rewrites the persisted fence value", async () => {
		const directory = await temporaryDirectory();
		const leaseDirectory = join(
			await realpath(directory),
			NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
		);
		const rawFence = "not-json:\u0000:byte-distinct";
		const seed = new ClassicLevel(leaseDirectory, {
			keyEncoding: "utf8",
			valueEncoding: "utf8",
		});
		await seed.open();
		await seed.put("fence", rawFence, { sync: true });
		await seed.close();

		const lock = await acquireNativeDurabilityNodeLock(directory);
		locks.add(lock);
		await lock.close();
		locks.delete(lock);

		const inspect = new ClassicLevel(leaseDirectory, {
			keyEncoding: "utf8",
			valueEncoding: "utf8",
		});
		await inspect.open();
		expect(await inspect.get("fence")).to.equal(rawFence);
		await inspect.close();
		expect(
			await rejectionOf(acquireNativeDurabilityNodeLease(directory)),
		).to.be.instanceOf(NativeDurabilityLeaseStateError);
	});

	it("fails closed and releases ownership when either directory sync fails", async () => {
		type FileHandlePrototype = {
			sync(): Promise<void>;
		};
		const failSyncAt = async (failureCall: 1 | 2): Promise<void> => {
			const directory = await temporaryDirectory();
			const canonicalDirectory = await realpath(directory);
			const sample = await openFile(directory, "r");
			const prototype = Object.getPrototypeOf(sample) as FileHandlePrototype;
			const sync = prototype.sync;
			await sample.close();
			const syncFailure = new Error(`injected sync failure ${failureCall}`);
			let syncCalls = 0;
			prototype.sync = async function () {
				syncCalls++;
				if (syncCalls === failureCall) {
					throw syncFailure;
				}
				await sync.call(this);
			};
			let acquisitionError: unknown;
			try {
				acquisitionError = await rejectionOf(
					acquireNativeDurabilityNodeLock(directory),
				);
			} finally {
				prototype.sync = sync;
			}
			expect(acquisitionError).to.be.instanceOf(
				NativeDurabilityLockDirectorySyncError,
			);
			expect(
				(acquisitionError as NativeDurabilityLockDirectorySyncError).directory,
			).to.equal(
				failureCall === 1
					? join(
							canonicalDirectory,
							NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
						)
					: canonicalDirectory,
			);

			const recovered = await acquireNativeDurabilityNodeLock(directory);
			locks.add(recovered);
			await recovered.close();
			locks.delete(recovered);
		};

		await failSyncAt(1);
		await failSyncAt(2);
	});

	it("preserves simultaneous sync and file-handle close failures", async () => {
		type FileHandlePrototype = {
			sync(): Promise<void>;
		};
		type FileHandle = FileHandlePrototype & {
			close: () => Promise<void>;
		};
		type MutableFsPromises = {
			open(path: string, flags: "r"): Promise<FileHandle>;
		};
		const directory = await temporaryDirectory();
		const sample = await openFile(directory, "r");
		const prototype = Object.getPrototypeOf(sample) as FileHandlePrototype;
		const sync = prototype.sync;
		await sample.close();
		const require = createRequire(import.meta.url);
		const fs = require("node:fs/promises") as MutableFsPromises;
		const open = fs.open;
		const syncFailure = new Error("injected simultaneous sync failure");
		const handleCloseFailure = new Error(
			"injected simultaneous handle close failure",
		);
		prototype.sync = async () => {
			throw syncFailure;
		};
		fs.open = async (path, flags) => {
			const handle = await open(path, flags);
			const close = handle.close.bind(handle);
			handle.close = async () => {
				await close();
				throw handleCloseFailure;
			};
			return handle;
		};
		syncBuiltinESMExports();
		let acquisitionError: unknown;
		try {
			acquisitionError = await rejectionOf(
				acquireNativeDurabilityNodeLock(directory),
			);
		} finally {
			prototype.sync = sync;
			fs.open = open;
			syncBuiltinESMExports();
		}
		expect(acquisitionError).to.be.instanceOf(AggregateError);
		const errors = (acquisitionError as { errors: unknown[] }).errors;
		expect(errors).to.have.length(2);
		expect(errors[0]).to.be.instanceOf(NativeDurabilityLockDirectorySyncError);
		expect(
			(errors[0] as NativeDurabilityLockDirectorySyncError).cause,
		).to.equal(syncFailure);
		expect(errors[1]).to.equal(handleCloseFailure);

		const recovered = await acquireNativeDurabilityNodeLock(directory);
		locks.add(recovered);
	});

	it("preserves acquisition and database cleanup failures", async () => {
		type FileHandlePrototype = {
			sync(): Promise<void>;
		};
		type CloseableDatabase = {
			close(): Promise<void>;
		};
		const directory = await temporaryDirectory();
		const sample = await openFile(directory, "r");
		const fileHandlePrototype = Object.getPrototypeOf(
			sample,
		) as FileHandlePrototype;
		const sync = fileHandlePrototype.sync;
		await sample.close();
		const databasePrototype = ClassicLevel.prototype as unknown as {
			close: (this: CloseableDatabase) => Promise<void>;
		};
		const databasePrototypeOwnedClose = Object.hasOwn(
			databasePrototype,
			"close",
		);
		const closeDatabase = databasePrototype.close;
		const syncFailure = new Error("injected acquisition sync failure");
		const databaseCloseFailure = new Error(
			"injected acquisition database close failure",
		);
		let failedDatabase: CloseableDatabase | undefined;
		fileHandlePrototype.sync = async () => {
			throw syncFailure;
		};
		databasePrototype.close = async function () {
			failedDatabase = this;
			throw databaseCloseFailure;
		};
		let acquisitionError: unknown;
		try {
			acquisitionError = await rejectionOf(
				acquireNativeDurabilityNodeLock(directory),
			);
		} finally {
			fileHandlePrototype.sync = sync;
			if (databasePrototypeOwnedClose) {
				databasePrototype.close = closeDatabase;
			} else {
				Reflect.deleteProperty(databasePrototype, "close");
			}
		}
		expect(acquisitionError).to.be.instanceOf(AggregateError);
		const errors = (acquisitionError as { errors: unknown[] }).errors;
		expect(errors).to.have.length(2);
		expect(errors[0]).to.be.instanceOf(NativeDurabilityLockDirectorySyncError);
		expect(
			(errors[0] as NativeDurabilityLockDirectorySyncError).cause,
		).to.equal(syncFailure);
		expect(errors[1]).to.equal(databaseCloseFailure);
		expect(
			await rejectionOf(acquireNativeDurabilityNodeLock(directory)),
		).to.be.instanceOf(NativeDurabilityLockUnavailableError);

		if (!failedDatabase) {
			throw new Error("Expected failed acquisition database to remain open");
		}
		await closeDatabase.call(failedDatabase);
		const recovered = await acquireNativeDurabilityNodeLock(directory);
		locks.add(recovered);
	});

	it("stays closed after its underlying close rejects", async () => {
		const directory = await temporaryDirectory();
		const lock = await acquireNativeDurabilityNodeLock(directory);
		locks.add(lock);
		const database = (
			lock as unknown as {
				database: { close: () => Promise<void> };
			}
		).database;
		const closeDatabase = database.close.bind(database);
		const closeFailure = new Error("injected close failure");
		database.close = async () => {
			throw closeFailure;
		};

		const closing = lock.close();
		expect(lock.close()).to.equal(closing);
		expect(await rejectionOf(closing)).to.equal(closeFailure);
		expect(lock.close()).to.equal(closing);
		expect(await rejectionOf(lock.assertHeld())).to.be.instanceOf(
			NativeDurabilityLockClosedError,
		);
		expect(
			await rejectionOf(lock.runWhileHeld(async () => undefined)),
		).to.be.instanceOf(NativeDurabilityLockClosedError);
		expect(
			await rejectionOf(acquireNativeDurabilityNodeLock(directory)),
		).to.be.instanceOf(NativeDurabilityLockUnavailableError);
		expect(
			await rejectionOf(acquireNativeDurabilityNodeLease(directory)),
		).to.be.instanceOf(NativeDurabilityLeaseUnavailableError);

		await closeDatabase();
		locks.delete(lock);
		const reopened = await acquireNativeDurabilityNodeLock(directory);
		locks.add(reopened);
	});

	it("reacquires immediately after SIGKILL", async function () {
		const directory = await temporaryDirectory();
		const child = fork(workerPath, ["hold", directory], {
			stdio: ["ignore", "ignore", "pipe", "ipc"],
		});
		children.add(child);
		const held = await waitForWorkerMessage(child);
		if (held.event === "error") {
			throw new Error(
				`Lock worker failed (${held.code ?? held.name ?? "unknown"}): ${held.message}`,
			);
		}

		expect(
			await rejectionOf(acquireNativeDurabilityNodeLock(directory)),
		).to.be.instanceOf(NativeDurabilityLockUnavailableError);
		expect(
			await rejectionOf(acquireNativeDurabilityNodeLease(directory)),
		).to.be.instanceOf(NativeDurabilityLeaseUnavailableError);

		const exited = once(child, "exit");
		expect(child.kill("SIGKILL")).to.equal(true);
		await exited;
		children.delete(child);

		const recovered = await acquireNativeDurabilityNodeLock(directory);
		locks.add(recovered);
	});
});
