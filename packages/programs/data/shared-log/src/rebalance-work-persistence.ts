import { toHexString } from "@peerbit/crypto";
import type {
	NativeBackboneCoordinatePersistenceStore,
	NativeDurabilityLease,
} from "@peerbit/native-backbone";
import {
	DEFAULT_REBALANCE_WORK_LIMITS,
	REBALANCE_WORK_FILES,
	type RebalanceWorkLimits,
	type RebalanceWorkPersistence,
	RebalanceWorkStore,
} from "./rebalance-work-store.js";

const REBALANCE_WORK_DIRECTORY = "rebalance-work";
const MAX_REBALANCE_WORK_LOG_ID_BYTES = 64;
const allowedFiles = new Set<string>(REBALANCE_WORK_FILES);
const openMemoryNamespaces = new WeakSet<Map<string, Uint8Array>>();

type NativeBackboneModule = Readonly<{
	NativeBackboneNodeCoordinatePersistenceStore: new (
		directory: string,
	) => NativeBackboneCoordinatePersistenceStore;
	acquireNativeDurabilityNodeLease(
		directory: string,
	): Promise<NativeDurabilityLease>;
}>;

export type RebalanceWorkNodeDirectoryHandle = Readonly<{
	sync(): Promise<void>;
	close(): Promise<void>;
}>;

export type RebalanceWorkNodeFileSystem = Readonly<{
	realpath(path: string): Promise<string>;
	stat(path: string): Promise<Readonly<{ isDirectory(): boolean }>>;
	mkdir(path: string): Promise<unknown>;
	open(path: string, flags: "r"): Promise<RebalanceWorkNodeDirectoryHandle>;
}>;

export type RebalanceWorkNodePath = Readonly<{
	join(...parts: string[]): string;
	dirname(path: string): string;
}>;

export type RebalanceWorkNodeModules = Readonly<{
	fs: RebalanceWorkNodeFileSystem;
	path: RebalanceWorkNodePath;
	native: NativeBackboneModule;
}>;

/** @internal Deterministic fault-injection seam for the direct adapter spec. */
export type RebalanceWorkNodePersistenceDependencies = Readonly<{
	loadNodeModules?(): Promise<RebalanceWorkNodeModules>;
	onPersistenceCreated?(persistence: RebalanceWorkPersistence): void;
}>;

export type OpenRebalanceWorkStoreOptions = Readonly<{
	limits?: Partial<RebalanceWorkLimits>;
}>;

export type OpenNodeRebalanceWorkStoreOptions = OpenRebalanceWorkStoreOptions &
	Readonly<{
		nodeDirectory: string;
		logId: Uint8Array;
	}>;

const dynamicImport = <T>(specifier: string): Promise<T> =>
	import(/* @vite-ignore */ specifier) as Promise<T>;

const validateFileName = (name: string): string => {
	if (!allowedFiles.has(name)) {
		throw new Error(`Invalid rebalance work persistence file: ${name}`);
	}
	return name;
};

const validateReadLimit = (maxBytes: number): number => {
	if (
		!Number.isSafeInteger(maxBytes) ||
		maxBytes < 0 ||
		maxBytes > DEFAULT_REBALANCE_WORK_LIMITS.maxFrameBytes
	) {
		throw new RangeError(
			`Rebalance work persistence read limit must be a non-negative safe integer no larger than ${DEFAULT_REBALANCE_WORK_LIMITS.maxFrameBytes}`,
		);
	}
	return maxBytes;
};

const captureWrite = (bytes: Uint8Array): Uint8Array => {
	if (!(bytes instanceof Uint8Array)) {
		throw new TypeError("Rebalance work persistence writes require bytes");
	}
	if (bytes.byteLength > DEFAULT_REBALANCE_WORK_LIMITS.maxFrameBytes) {
		throw new RangeError(
			`Rebalance work persistence write exceeds ${DEFAULT_REBALANCE_WORK_LIMITS.maxFrameBytes} bytes`,
		);
	}
	return new Uint8Array(bytes);
};

const captureLimits = (
	limits: Partial<RebalanceWorkLimits> | undefined,
): RebalanceWorkLimits => {
	const captured = {
		...DEFAULT_REBALANCE_WORK_LIMITS,
		...limits,
	};
	for (const key of Object.keys(
		DEFAULT_REBALANCE_WORK_LIMITS,
	) as (keyof RebalanceWorkLimits)[]) {
		const value = captured[key];
		if (
			!Number.isSafeInteger(value) ||
			value <= 0 ||
			value > DEFAULT_REBALANCE_WORK_LIMITS[key]
		) {
			throw new RangeError(
				`Rebalance work limit ${key} must be a positive safe integer no larger than ${DEFAULT_REBALANCE_WORK_LIMITS[key]}`,
			);
		}
	}
	return Object.freeze(captured);
};

const closeAfterOpenFailure = async (
	persistence: RebalanceWorkPersistence,
	error: unknown,
): Promise<never> => {
	let closeFailed = false;
	let closeError: unknown;
	try {
		await persistence.close?.({ flush: false });
	} catch (cleanupError) {
		closeFailed = true;
		closeError = cleanupError;
	}
	if (closeFailed) {
		throw new AggregateError(
			[error, closeError],
			"Failed to open and close rebalance work persistence",
		);
	}
	throw error;
};

/**
 * In-process restart persistence for tests and non-persistent nodes. A backing
 * map is exclusively owned until close and may then be supplied to a new
 * adapter. It is never a strict durability capability.
 */
export class RebalanceWorkMemoryPersistence
	implements RebalanceWorkPersistence
{
	private closed = false;
	private closePromise?: Promise<void>;

	constructor(readonly files: Map<string, Uint8Array> = new Map()) {
		if (!(files instanceof Map)) {
			throw new TypeError("Rebalance work memory persistence requires a Map");
		}
		if (openMemoryNamespaces.has(files)) {
			throw new Error("Rebalance work memory persistence is already open");
		}
		openMemoryNamespaces.add(files);
	}

	async read(name: string, maxBytes: number): Promise<Uint8Array | undefined> {
		this.assertOpen();
		const validName = validateFileName(name);
		const limit = validateReadLimit(maxBytes);
		const bytes = this.files.get(validName);
		if (bytes && bytes.byteLength > limit) {
			throw new RangeError(
				`Rebalance work persistence file ${validName} exceeds the ${limit} byte read limit`,
			);
		}
		return bytes ? new Uint8Array(bytes) : undefined;
	}

	async write(name: string, bytes: Uint8Array): Promise<void> {
		this.assertOpen();
		this.files.set(validateFileName(name), captureWrite(bytes));
	}

	close(): Promise<void> {
		if (this.closePromise) {
			return this.closePromise;
		}
		this.closed = true;
		openMemoryNamespaces.delete(this.files);
		this.closePromise = Promise.resolve();
		return this.closePromise;
	}

	private assertOpen(): void {
		if (this.closed) {
			throw new Error("Rebalance work memory persistence is closed");
		}
	}
}

export const openMemoryRebalanceWorkStore = async (
	options: OpenRebalanceWorkStoreOptions &
		Readonly<{ files?: Map<string, Uint8Array> }> = {},
): Promise<RebalanceWorkStore> => {
	const limits = captureLimits(options.limits);
	const persistence = new RebalanceWorkMemoryPersistence(options.files);
	try {
		return await RebalanceWorkStore.open({
			persistence,
			durability: "memory",
			limits,
		});
	} catch (error) {
		return closeAfterOpenFailure(persistence, error);
	}
};

const hasErrorCode = (error: unknown, code: string): boolean => {
	const seen = new Set<unknown>();
	let current = error;
	while (current != null && typeof current === "object" && !seen.has(current)) {
		seen.add(current);
		if ((current as { code?: unknown }).code === code) {
			return true;
		}
		current = (current as { cause?: unknown }).cause;
	}
	return false;
};

const syncDirectoryStrict = async (
	fs: RebalanceWorkNodeFileSystem,
	directory: string,
): Promise<void> => {
	let handle: RebalanceWorkNodeDirectoryHandle | undefined;
	let operationFailed = false;
	let operationError: unknown;
	try {
		handle = await fs.open(directory, "r");
		await handle.sync();
	} catch (error) {
		operationFailed = true;
		operationError = error;
	}
	let closeFailed = false;
	let closeError: unknown;
	try {
		await handle?.close();
	} catch (error) {
		closeFailed = true;
		closeError = error;
	}
	if (operationFailed && closeFailed) {
		throw new AggregateError(
			[operationError, closeError],
			`Failed to sync and close rebalance work directory ${directory}`,
		);
	}
	if (operationFailed) throw operationError;
	if (closeFailed) throw closeError;
};

const ensureCanonicalChildDirectory = async (
	fs: RebalanceWorkNodeFileSystem,
	path: RebalanceWorkNodePath,
	canonicalParent: string,
	name: string,
): Promise<string> => {
	const requested = path.join(canonicalParent, name);
	try {
		await fs.mkdir(requested);
	} catch (error) {
		if (!hasErrorCode(error, "EEXIST")) {
			throw error;
		}
	}
	const facts = await fs.stat(requested);
	if (!facts.isDirectory()) {
		throw new Error(`Rebalance work path is not a directory: ${requested}`);
	}
	// Repeat this on every open, including EEXIST retries. A previous creation
	// may have become visible before its parent-directory sync failed.
	await syncDirectoryStrict(fs, canonicalParent);
	const canonical = await fs.realpath(requested);
	if (canonical !== requested || path.dirname(canonical) !== canonicalParent) {
		throw new Error(
			`Rebalance work directory is not its canonical requested path: ${canonical}`,
		);
	}
	return canonical;
};

const loadNodeModules = async (): Promise<RebalanceWorkNodeModules> => {
	const processLike = (
		globalThis as { process?: { versions?: { node?: string } } }
	).process;
	if (!processLike?.versions?.node) {
		throw new Error("Persistent rebalance work is only supported in Node.js");
	}
	const [fs, path, native] = await Promise.all([
		dynamicImport<RebalanceWorkNodeFileSystem>("node:fs/promises"),
		dynamicImport<RebalanceWorkNodePath>("node:path"),
		dynamicImport<NativeBackboneModule>("@peerbit/native-backbone"),
	]);
	return { fs, path, native };
};

const captureNodeOptions = (
	options: OpenNodeRebalanceWorkStoreOptions,
): Readonly<{
	nodeDirectory: string;
	logId: Uint8Array;
	limits: RebalanceWorkLimits;
}> => {
	if (typeof options?.nodeDirectory !== "string" || !options.nodeDirectory) {
		throw new TypeError("nodeDirectory must be a non-empty string");
	}
	if (
		!(options.logId instanceof Uint8Array) ||
		options.logId.byteLength === 0 ||
		options.logId.byteLength > MAX_REBALANCE_WORK_LOG_ID_BYTES
	) {
		throw new TypeError(
			`logId must contain 1-${MAX_REBALANCE_WORK_LOG_ID_BYTES} bytes`,
		);
	}
	return Object.freeze({
		nodeDirectory: options.nodeDirectory,
		logId: new Uint8Array(options.logId),
		limits: captureLimits(options.limits),
	});
};

class NodeRebalanceWorkPersistence implements RebalanceWorkPersistence {
	private tail: Promise<void> = Promise.resolve();
	private closing = false;
	private closed = false;
	private closePromise?: Promise<void>;

	constructor(
		private readonly raw: NativeBackboneCoordinatePersistenceStore,
		private readonly lease: NativeDurabilityLease,
		private readonly fs: RebalanceWorkNodeFileSystem,
		private readonly namespace: string,
	) {}

	read(name: string, maxBytes: number): Promise<Uint8Array | undefined> {
		this.assertAccepting();
		const validName = validateFileName(name);
		const limit = validateReadLimit(maxBytes);
		return this.enqueue(() => this.raw.read(validName, limit));
	}

	write(name: string, bytes: Uint8Array): Promise<void> {
		this.assertAccepting();
		const validName = validateFileName(name);
		const captured = captureWrite(bytes);
		return this.enqueue(() => this.raw.write(validName, captured));
	}

	durableBarrier(name?: string): Promise<void> {
		this.assertAccepting();
		const validName = validateFileName(name ?? "");
		return this.enqueue(async () => {
			if (typeof this.raw.durableBarrier !== "function") {
				throw new Error(
					"Node rebalance work persistence requires a named file barrier",
				);
			}
			await this.raw.durableBarrier(validName);
			// The coordinate adapter intentionally tolerates unsupported directory
			// syncs. Strict work frames cannot: first-file namespace metadata must be
			// durable before the barrier can authorize a restart checkpoint.
			await syncDirectoryStrict(this.fs, this.namespace);
		});
	}

	flush(name?: string): Promise<void> {
		this.assertAccepting();
		const validName = name === undefined ? undefined : validateFileName(name);
		return this.enqueue(async () => {
			await this.raw.flush?.(validName);
		});
	}

	close(_options?: { flush?: boolean }): Promise<void> {
		if (this.closePromise) {
			return this.closePromise;
		}
		this.closing = true;
		this.closePromise = (async () => {
			await this.tail;
			let rawFailed = false;
			let rawError: unknown;
			try {
				await this.lease.runWhileHeld(async () => {
					await this.raw.close?.({ flush: false });
				});
			} catch (error) {
				rawFailed = true;
				rawError = error;
			}
			let leaseFailed = false;
			let leaseError: unknown;
			try {
				await this.lease.close();
			} catch (error) {
				leaseFailed = true;
				leaseError = error;
			} finally {
				this.closed = true;
			}
			if (rawFailed && leaseFailed) {
				throw new AggregateError(
					[rawError, leaseError],
					"Failed to close rebalance work persistence and lease",
				);
			}
			if (rawFailed) throw rawError;
			if (leaseFailed) throw leaseError;
		})();
		return this.closePromise;
	}

	private enqueue<T>(operation: () => Promise<T>): Promise<T> {
		this.assertAccepting();
		const result = this.tail.then(() => this.lease.runWhileHeld(operation));
		this.tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	private assertAccepting(): void {
		if (this.closing || this.closed) {
			throw new Error("Rebalance work persistence is closing");
		}
	}
}

const createNodePersistence = async (
	options: ReturnType<typeof captureNodeOptions>,
	dependencies: RebalanceWorkNodePersistenceDependencies,
): Promise<NodeRebalanceWorkPersistence> => {
	const { fs, path, native } = await (
		dependencies.loadNodeModules ?? loadNodeModules
	)();
	const canonicalRoot = await fs.realpath(options.nodeDirectory);
	const rootFacts = await fs.stat(canonicalRoot);
	if (!rootFacts.isDirectory()) {
		throw new Error(
			`Peerbit node directory is not a directory: ${canonicalRoot}`,
		);
	}
	const workRoot = await ensureCanonicalChildDirectory(
		fs,
		path,
		canonicalRoot,
		REBALANCE_WORK_DIRECTORY,
	);
	const namespace = await ensureCanonicalChildDirectory(
		fs,
		path,
		workRoot,
		toHexString(options.logId),
	);
	// Probe strict directory-sync support before acquiring the lifetime lock.
	await syncDirectoryStrict(fs, namespace);

	const lease = await native.acquireNativeDurabilityNodeLease(namespace);
	let raw: NativeBackboneCoordinatePersistenceStore | undefined;
	try {
		raw = new native.NativeBackboneNodeCoordinatePersistenceStore(namespace);
		if (typeof raw.durableBarrier !== "function") {
			throw new Error(
				"Native Node persistence does not expose a physical durability barrier",
			);
		}
		return new NodeRebalanceWorkPersistence(raw, lease, fs, namespace);
	} catch (error) {
		const errors: unknown[] = [error];
		if (raw) {
			try {
				await lease.runWhileHeld(async () => {
					await raw!.close?.({ flush: false });
				});
			} catch (cleanupError) {
				errors.push(cleanupError);
			}
		}
		try {
			await lease.close();
		} catch (cleanupError) {
			errors.push(cleanupError);
		}
		if (errors.length > 1) {
			throw new AggregateError(
				errors,
				"Failed to construct rebalance work persistence and close its lease",
			);
		}
		throw error;
	}
};

/**
 * Open one strict Node work store atomically with its lifetime namespace lease.
 * The leased persistence adapter is intentionally not exposed, so every failed
 * store preflight and recovery path can release ownership before returning.
 * This does not validate placement-view currency and grants no prune authority.
 */
export const openNodeRebalanceWorkStore = async (
	input: OpenNodeRebalanceWorkStoreOptions,
	dependencies: RebalanceWorkNodePersistenceDependencies = {},
): Promise<RebalanceWorkStore> => {
	const options = captureNodeOptions(input);
	const persistence = await createNodePersistence(options, dependencies);
	try {
		dependencies.onPersistenceCreated?.(persistence);
		return await RebalanceWorkStore.open({
			persistence,
			durability: "strict",
			limits: options.limits,
		});
	} catch (error) {
		return closeAfterOpenFailure(persistence, error);
	}
};
