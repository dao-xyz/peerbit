import {
	NATIVE_DURABILITY_MAX_U64,
	NATIVE_DURABILITY_MAX_WRITER_ID_BYTES,
	type NativeDurabilityFence,
	NativeDurabilityFenceExhaustedError,
	type NativeDurabilityLease,
	NativeDurabilityLeaseClosedError,
	NativeDurabilityLeaseDirectorySyncError,
	NativeDurabilityLeaseStateError,
	NativeDurabilityLeaseUnavailableError,
} from "./lease.js";

export const NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME =
	".peerbit-native-durability-lease";
const LEASE_STATE_KEY = "fence";
const LEASE_STATE_VERSION = 1;

type PersistedLeaseState = {
	version: typeof LEASE_STATE_VERSION;
	epoch: string;
	ownerId: string;
	domainId: string;
};

type NativeLevelDatabase = {
	readonly status: string;
	open(): Promise<void>;
	get(key: string): Promise<string | undefined>;
	put(key: string, value: string, options: { sync: true }): Promise<void>;
	close(): Promise<void>;
};

type ClassicLevelConstructor = new (
	location: string,
	options: { keyEncoding: "utf8"; valueEncoding: "utf8" },
) => NativeLevelDatabase;

type NodeFsPromises = {
	realpath(path: string): Promise<string>;
	open(
		path: string,
		flags: "r",
	): Promise<{
		sync(): Promise<void>;
		close(): Promise<void>;
	}>;
};

type NodePath = {
	join(...parts: string[]): string;
};

type NodeCrypto = {
	randomUUID(): string;
};

const dynamicImport = <T>(specifier: string): Promise<T> =>
	import(/* @vite-ignore */ specifier) as Promise<T>;

const importClassicLevel = async (): Promise<ClassicLevelConstructor> => {
	const module = await dynamicImport<{ ClassicLevel: ClassicLevelConstructor }>(
		"classic-level",
	);
	return module.ClassicLevel;
};

const hasErrorCode = (error: unknown, expected: string): boolean => {
	const visited = new Set<unknown>();
	let current = error;
	while (
		current != null &&
		typeof current === "object" &&
		!visited.has(current)
	) {
		visited.add(current);
		if ((current as { code?: unknown }).code === expected) {
			return true;
		}
		current = (current as { cause?: unknown }).cause;
	}
	return false;
};

const isMissingKeyError = (error: unknown): boolean =>
	hasErrorCode(error, "LEVEL_NOT_FOUND");

const decodeLeaseState = (
	encoded: string,
	directory: string,
): PersistedLeaseState => {
	let decoded: unknown;
	try {
		decoded = JSON.parse(encoded);
	} catch (error) {
		throw new NativeDurabilityLeaseStateError(
			directory,
			`Invalid native durability lease state in ${directory}`,
			{ cause: error },
		);
	}
	const value = decoded as Partial<PersistedLeaseState> | null;
	if (
		value?.version !== LEASE_STATE_VERSION ||
		typeof value.epoch !== "string" ||
		!/^(0|[1-9][0-9]*)$/.test(value.epoch) ||
		BigInt(value.epoch) > NATIVE_DURABILITY_MAX_U64 ||
		typeof value.ownerId !== "string" ||
		value.ownerId.length === 0 ||
		new TextEncoder().encode(value.ownerId).byteLength >
			NATIVE_DURABILITY_MAX_WRITER_ID_BYTES ||
		typeof value.domainId !== "string" ||
		value.domainId.length === 0 ||
		new TextEncoder().encode(value.domainId).byteLength >
			NATIVE_DURABILITY_MAX_WRITER_ID_BYTES
	) {
		throw new NativeDurabilityLeaseStateError(
			directory,
			`Invalid native durability lease state in ${directory}`,
		);
	}
	return value as PersistedLeaseState;
};

const readLeaseState = async (
	database: NativeLevelDatabase,
	directory: string,
): Promise<PersistedLeaseState | undefined> => {
	try {
		const encoded = await database.get(LEASE_STATE_KEY);
		return encoded == null ? undefined : decodeLeaseState(encoded, directory);
	} catch (error) {
		if (isMissingKeyError(error)) {
			return undefined;
		}
		throw error;
	}
};

const syncDirectory = async (
	fs: NodeFsPromises,
	directory: string,
): Promise<void> => {
	let handle: Awaited<ReturnType<NodeFsPromises["open"]>> | undefined;
	try {
		handle = await fs.open(directory, "r");
		await handle.sync();
	} catch (error) {
		throw new NativeDurabilityLeaseDirectorySyncError(directory, {
			cause: error,
		});
	} finally {
		await handle?.close();
	}
};

class NodeNativeDurabilityLease implements NativeDurabilityLease {
	private state: "held" | "closing" | "closed" = "held";
	private closePromise?: Promise<void>;
	private activeOperations = 0;
	private drainPromise?: Promise<void>;
	private resolveDrain?: () => void;

	constructor(
		readonly fence: NativeDurabilityFence,
		private readonly database: NativeLevelDatabase,
	) {}

	async assertHeld(): Promise<void> {
		if (this.state !== "held" || this.database.status !== "open") {
			throw new NativeDurabilityLeaseClosedError(this.fence);
		}
	}

	async runWhileHeld<T>(operation: () => Promise<T>): Promise<T> {
		if (this.state !== "held" || this.database.status !== "open") {
			throw new NativeDurabilityLeaseClosedError(this.fence);
		}
		// The state check and increment are synchronous, so close() cannot release
		// the database lock between them.
		this.activeOperations++;
		try {
			return await operation();
		} finally {
			this.activeOperations--;
			if (this.activeOperations === 0) {
				this.resolveDrain?.();
				this.resolveDrain = undefined;
				this.drainPromise = undefined;
			}
		}
	}

	close(): Promise<void> {
		if (this.closePromise) {
			return this.closePromise;
		}
		this.state = "closing";
		if (this.activeOperations > 0 && !this.drainPromise) {
			this.drainPromise = new Promise<void>((resolve) => {
				this.resolveDrain = resolve;
			});
		}
		this.closePromise = (this.drainPromise ?? Promise.resolve())
			.then(() => this.database.close())
			.finally(() => {
				this.state = "closed";
			});
		return this.closePromise;
	}
}

/**
 * Acquire the crash-released Node ownership lease for an existing program
 * directory.
 *
 * The dedicated ClassicLevel database lives beside, rather than inside, the
 * transaction namespace. Its native LevelDB `LOCK` is held for this object's
 * lifetime and is released by the operating system when the process dies. The
 * lock database itself is intentionally retained so fence epochs cannot be
 * reused after a normal reopen.
 */
export const acquireNativeDurabilityNodeLease = async (
	programDirectory: string,
): Promise<NativeDurabilityLease> => {
	const [fs, path, crypto, ClassicLevel] = await Promise.all([
		dynamicImport<NodeFsPromises>("node:fs/promises"),
		dynamicImport<NodePath>("node:path"),
		dynamicImport<NodeCrypto>("node:crypto"),
		importClassicLevel(),
	]);
	let canonicalDirectory: string;
	try {
		canonicalDirectory = await fs.realpath(programDirectory);
	} catch (error) {
		throw new NativeDurabilityLeaseStateError(
			programDirectory,
			`Native durability program directory must already exist: ${programDirectory}`,
			{ cause: error },
		);
	}
	const leaseDirectory = path.join(
		canonicalDirectory,
		NATIVE_DURABILITY_NODE_LEASE_DIRECTORY_NAME,
	);
	const database = new ClassicLevel(leaseDirectory, {
		keyEncoding: "utf8",
		valueEncoding: "utf8",
	});

	try {
		await database.open();
	} catch (error) {
		if (hasErrorCode(error, "LEVEL_LOCKED")) {
			throw new NativeDurabilityLeaseUnavailableError(canonicalDirectory, {
				cause: error,
			});
		}
		throw error;
	}

	try {
		const previous = await readLeaseState(database, canonicalDirectory);
		if (BigInt(previous?.epoch ?? "0") === NATIVE_DURABILITY_MAX_U64) {
			throw new NativeDurabilityFenceExhaustedError(canonicalDirectory);
		}
		const fence: NativeDurabilityFence = Object.freeze({
			epoch: BigInt(previous?.epoch ?? "0") + 1n,
			ownerId: crypto.randomUUID(),
			domainId: previous?.domainId ?? crypto.randomUUID(),
		});
		const next: PersistedLeaseState = {
			version: LEASE_STATE_VERSION,
			epoch: fence.epoch.toString(),
			ownerId: fence.ownerId,
			domainId: fence.domainId,
		};
		await database.put(LEASE_STATE_KEY, JSON.stringify(next), { sync: true });
		// LevelDB's sync option covers its value/WAL. Explicit directory syncs
		// additionally make creation of the lease database and its files durable
		// before the fence is handed to a transaction writer.
		await syncDirectory(fs, leaseDirectory);
		await syncDirectory(fs, canonicalDirectory);
		return new NodeNativeDurabilityLease(fence, database);
	} catch (error) {
		try {
			await database.close();
		} catch {
			// The acquisition error is authoritative. A failed close must not make
			// the partially initialized lease usable by this process.
		}
		throw error;
	}
};
