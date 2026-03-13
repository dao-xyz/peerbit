import { field, serialize, variant } from "@dao-xyz/borsh";
import { OPFSStore } from "@peerbit/any-store-opfs/opfs-store";
import { Documents } from "@peerbit/document";
import { documentAdapter } from "@peerbit/document-proxy/auto";
import { useQuery } from "@peerbit/document-react";
import { PeerProvider, usePeer, useProgram } from "@peerbit/react";
import React from "react";
import { CanonicalPost } from "./canonical-types";

const getBootstrapAddrs = () => {
	const search = new URLSearchParams(window.location.search);
	const params = search.getAll("bootstrap");
	const fromComma = params.flatMap((p) =>
		p
			.split(",")
			.map((x) => x.trim())
			.filter(Boolean),
	);
	return fromComma;
};

const getSqliteEnabled = () =>
	new URLSearchParams(window.location.search).has("sqlite");

const getDocumentQueryEnabled = () =>
	new URLSearchParams(window.location.search).has("doc");

const getCanonicalEnabled = () =>
	new URLSearchParams(window.location.search).has("canonical");

const getOPFSBenchmarkEnabled = () =>
	new URLSearchParams(window.location.search).has("opfsbench");

const getDocumentBenchmarkEnabled = () =>
	new URLSearchParams(window.location.search).has("docbench");

const getSQLiteBenchmarkEnabled = () =>
	new URLSearchParams(window.location.search).has("sqlitebench");

const getInMemoryEnabled = () =>
	new URLSearchParams(window.location.search).has("inmemory");

const getSimpleIndexEnabled = () =>
	new URLSearchParams(window.location.search).has("simpleindex");

const getDocumentIndexMode = (): "full" | "meta" => {
	const mode = new URLSearchParams(window.location.search).get("docindex");
	return mode === "meta" ? "meta" : "full";
};

const getSQLiteProtocol = (): SqliteWorkerProtocol | undefined => {
	const protocol = new URLSearchParams(window.location.search).get(
		"sqliteprotocol",
	);
	return protocol === "legacy" || protocol === "clone" ? protocol : undefined;
};

type SqliteWorkerProtocol = "legacy" | "clone";
type SQLiteSynchronousMode = "FULL" | "NORMAL" | "OFF";

const getSQLiteSynchronous = (): SQLiteSynchronousMode | undefined => {
	const mode =
		new URLSearchParams(window.location.search).get("sqlitesynchronous");
	if (!mode) {
		return undefined;
	}
	const normalized = mode.toUpperCase();
	return normalized === "FULL" ||
		normalized === "NORMAL" ||
		normalized === "OFF"
		? normalized
		: undefined;
};

type SQLiteProfileSample = {
	requestType: string;
	protocol: SqliteWorkerProtocol;
	databaseId: string;
	databaseDirectory?: string;
	sql?: string;
	clientEncodeMs: number;
	clientRoundTripMs: number;
	valueCount: number;
	blobValueCount: number;
	blobBytes: number;
	worker?: {
		decodeMs: number;
		execMs: number;
		totalMs: number;
		valueCount: number;
		blobValueCount: number;
		blobBytes: number;
	};
};

type SQLiteDatabaseLike = {
	exec: (sql: string) => Promise<any>;
	prepare: (sql: string, id?: string) => Promise<{
		run: (values: any[]) => Promise<void>;
		get: (values?: any[]) => Promise<Record<string, unknown> | undefined>;
		finalize?: () => Promise<void>;
	}>;
	drop: () => Promise<void>;
};

type SQLiteCreateOptions = {
	protocol?: SqliteWorkerProtocol;
	pragmas?: {
		synchronous?: SQLiteSynchronousMode;
	};
	profile?: boolean;
	onProfile?: (sample: SQLiteProfileSample) => void;
};

const getNumberParam = (key: string, fallback: number) => {
	const value = Number.parseInt(
		new URLSearchParams(window.location.search).get(key) ?? "",
		10,
	);
	return Number.isFinite(value) && value > 0 ? value : fallback;
};

const SqliteStatus = () => {
	const enabled = React.useMemo(getSqliteEnabled, []);
	const [status, setStatus] = React.useState("idle");

	React.useEffect(() => {
		if (!enabled) {
			return;
		}
		let cancelled = false;
		const run = async () => {
			setStatus("loading");
			try {
				const { create } = await import("@peerbit/indexer-sqlite3");
				await create();
				if (!cancelled) {
					setStatus("ready");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setStatus("error");
				}
			}
		};
		void run();
		return () => {
			cancelled = true;
		};
	}, [enabled]);

	if (!enabled) {
		return null;
	}

	return <div data-testid="sqlite-status">{status}</div>;
};

@variant(0)
class Post {
	@field({ type: "string" })
	id!: string;

	@field({ type: "string" })
	message!: string;

	constructor(props?: { id?: string; message?: string }) {
		if (!props) return; // borsh
		this.id = props.id ?? `${Date.now()}-${Math.random()}`;
		this.message = props.message ?? "";
	}
}

@variant(0)
class PostIndexed {
	@field({ type: "string" })
	id!: string;

	@field({ type: "string" })
	indexedMessage!: string;

	constructor(props?: Post) {
		if (!props) return; // borsh
		this.id = props.id;
		this.indexedMessage = props.message;
	}
}

const NodeDocumentQueryStatus = () => {
	const { peer } = usePeer();
	const docs = React.useMemo(() => {
		return new Documents<Post, PostIndexed>();
	}, []);
	const queryDefinition = React.useMemo(
		() => ({
			query: {
				indexedMessage: "hello",
			},
		}),
		[],
	);

	const { program, loading } = useProgram(
		peer,
		peer && docs ? docs : undefined,
		{
			args: {
				type: Post,
				index: {
					type: PostIndexed,
				},
				replicate: false,
			},
			keepOpenOnUnmount: true,
		},
	);

	const [seedState, setSeedState] = React.useState<
		"idle" | "seeding" | "seeded" | "error"
	>("idle");
	const hasSeededRef = React.useRef(false);

	React.useEffect(() => {
		if (!program || hasSeededRef.current) {
			return;
		}
		hasSeededRef.current = true;

		let cancelled = false;
		const run = async () => {
			setSeedState("seeding");
			try {
				await program.put(new Post({ id: "a", message: "hello" }));
				await program.put(new Post({ id: "b", message: "bye" }));
				if (!cancelled) {
					setSeedState("seeded");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setSeedState("error");
				}
			}
		};
		void run();

		return () => {
			cancelled = true;
		};
	}, [program]);

	const query = useQuery(seedState === "seeded" ? program : undefined, {
		query: queryDefinition,
		resolve: true,
		local: true,
		prefetch: true,
	});

	const queryItems = query.items ?? [];

	let status = "idle";
	if (seedState === "error") {
		status = "error";
	} else if (loading || !program) {
		status = "opening";
	} else if (seedState !== "seeded") {
		status = "seeding";
	} else if (query.isLoading) {
		status = "querying";
	} else if (queryItems.length === 1 && queryItems[0]?.message === "hello") {
		status = "ready";
	} else {
		status = "waiting";
	}

	return (
		<div>
			<div data-testid="doc-query-status">{status}</div>
			<ul data-testid="doc-query-results">
				{queryItems.map((item) => (
					<li key={item.id}>{item.message}</li>
				))}
			</ul>
		</div>
	);
};

const CanonicalDocumentQueryStatus = () => {
	const { peer } = usePeer();
	const docs = React.useMemo(() => new Documents<CanonicalPost>(), []);
	const queryDefinition = React.useMemo(
		() => ({
			query: {
				message: "hello",
			},
		}),
		[],
	);

	const { program, loading } = useProgram(peer, peer ? docs : undefined, {
		args: {
			type: CanonicalPost,
			replicate: false,
		},
		keepOpenOnUnmount: true,
	});

	const [seedState, setSeedState] = React.useState<
		"idle" | "seeding" | "seeded" | "error"
	>("idle");
	const hasSeededRef = React.useRef(false);

	React.useEffect(() => {
		if (!program || hasSeededRef.current) {
			return;
		}
		hasSeededRef.current = true;

		let cancelled = false;
		const run = async () => {
			setSeedState("seeding");
			try {
				await program.put(new CanonicalPost({ id: "a", message: "hello" }));
				await program.put(new CanonicalPost({ id: "b", message: "bye" }));
				if (!cancelled) {
					setSeedState("seeded");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setSeedState("error");
				}
			}
		};
		void run();

		return () => {
			cancelled = true;
		};
	}, [program]);

	const query = useQuery(seedState === "seeded" ? program : undefined, {
		query: queryDefinition,
		resolve: true,
		local: true,
		prefetch: true,
	});

	const queryItems = query.items ?? [];

	let status = "idle";
	if (seedState === "error") {
		status = "error";
	} else if (loading || !program) {
		status = "opening";
	} else if (seedState !== "seeded") {
		status = "seeding";
	} else if (query.isLoading) {
		status = "querying";
	} else if (queryItems.length === 1 && queryItems[0]?.message === "hello") {
		status = "ready";
	} else {
		status = "waiting";
	}

	return (
		<div>
			<div data-testid="doc-query-status">{status}</div>
			<ul data-testid="doc-query-results">
				{queryItems.map((item) => (
					<li key={item.id}>{item.message}</li>
				))}
			</ul>
		</div>
	);
};

const DocumentQueryStatus = () => {
	const enabled = React.useMemo(getDocumentQueryEnabled, []);
	const canonical = React.useMemo(getCanonicalEnabled, []);

	if (!enabled) {
		return null;
	}

	return canonical ? (
		<CanonicalDocumentQueryStatus />
	) : (
		<NodeDocumentQueryStatus />
	);
};

type SQLiteProfileSummary = {
	totalRequests: number;
	totalClientEncodeMs: number;
	totalClientRoundTripMs: number;
	totalWorkerDecodeMs: number;
	totalWorkerExecMs: number;
	totalBlobBytes: number;
	byType: Record<
		string,
		{
			count: number;
			clientEncodeMs: number;
			clientRoundTripMs: number;
			workerDecodeMs: number;
			workerExecMs: number;
			blobBytes: number;
		}
	>;
	byStatement: Record<
		string,
		{
			count: number;
			requestType: string;
			databaseDirectory?: string;
			sql?: string;
			clientEncodeMs: number;
			clientRoundTripMs: number;
			workerDecodeMs: number;
			workerExecMs: number;
			blobBytes: number;
		}
	>;
};

const summarizeSQLiteProfiles = (
	samples: SQLiteProfileSample[],
): SQLiteProfileSummary => {
	const byType: SQLiteProfileSummary["byType"] = {};
	const byStatement: SQLiteProfileSummary["byStatement"] = {};
	let totalClientEncodeMs = 0;
	let totalClientRoundTripMs = 0;
	let totalWorkerDecodeMs = 0;
	let totalWorkerExecMs = 0;
	let totalBlobBytes = 0;

	for (const sample of samples) {
		totalClientEncodeMs += sample.clientEncodeMs;
		totalClientRoundTripMs += sample.clientRoundTripMs;
		totalWorkerDecodeMs += sample.worker?.decodeMs ?? 0;
		totalWorkerExecMs += sample.worker?.execMs ?? 0;
		totalBlobBytes += sample.blobBytes;

		const bucket = (byType[sample.requestType] ??= {
			count: 0,
			clientEncodeMs: 0,
			clientRoundTripMs: 0,
			workerDecodeMs: 0,
			workerExecMs: 0,
			blobBytes: 0,
		});
		bucket.count += 1;
		bucket.clientEncodeMs += sample.clientEncodeMs;
		bucket.clientRoundTripMs += sample.clientRoundTripMs;
		bucket.workerDecodeMs += sample.worker?.decodeMs ?? 0;
		bucket.workerExecMs += sample.worker?.execMs ?? 0;
		bucket.blobBytes += sample.blobBytes;

		const statementKey = [
			sample.databaseDirectory ?? sample.databaseId,
			sample.requestType,
			sample.sql ?? "(none)",
		].join(" | ");
		const statementBucket = (byStatement[statementKey] ??= {
			count: 0,
			requestType: sample.requestType,
			databaseDirectory: sample.databaseDirectory,
			sql: sample.sql,
			clientEncodeMs: 0,
			clientRoundTripMs: 0,
			workerDecodeMs: 0,
			workerExecMs: 0,
			blobBytes: 0,
		});
		statementBucket.count += 1;
		statementBucket.clientEncodeMs += sample.clientEncodeMs;
		statementBucket.clientRoundTripMs += sample.clientRoundTripMs;
		statementBucket.workerDecodeMs += sample.worker?.decodeMs ?? 0;
		statementBucket.workerExecMs += sample.worker?.execMs ?? 0;
		statementBucket.blobBytes += sample.blobBytes;
	}

	return {
		totalRequests: samples.length,
		totalClientEncodeMs,
		totalClientRoundTripMs,
		totalWorkerDecodeMs,
		totalWorkerExecMs,
		totalBlobBytes,
		byType,
		byStatement,
	};
};

type OPFSBenchmarkResult = {
	protocol: "clone" | "legacy";
	putMs: number;
	getMs: number;
	size: number;
	bytes: number;
	count: number;
};

const runOPFSBenchmark = async ({
	protocol,
	bytes,
	count,
}: {
	protocol: "clone" | "legacy";
	bytes: number;
	count: number;
}): Promise<OPFSBenchmarkResult> => {
	const directory = `opfs-bench-${protocol}-${Date.now()}-${Math.random()
		.toString(16)
		.slice(2)}`;
	const store = new OPFSStore(directory, { protocol });
	await store.open();
	const template = new Uint8Array(bytes);
	for (let i = 0; i < template.length; i++) {
		template[i] = i % 251;
	}
	try {
		const putStart = performance.now();
		for (let i = 0; i < count; i++) {
			const value = template.slice();
			value[0] = i % 251;
			await store.put(`key-${i}`, value);
		}
		const putMs = performance.now() - putStart;

		const getStart = performance.now();
		for (let i = 0; i < count; i++) {
			const value = await store.get(`key-${i}`);
			if (!value || value.length !== bytes || value[0] !== i % 251) {
				throw new Error(`Unexpected OPFS value for key-${i}`);
			}
		}
		const getMs = performance.now() - getStart;

		return {
			protocol,
			putMs,
			getMs,
			size: await store.size(),
			bytes,
			count,
		};
	} finally {
		await store.clear();
		await store.close();
	}
};

const OPFSBenchmarkStatus = () => {
	const enabled = React.useMemo(getOPFSBenchmarkEnabled, []);
	const bytes = React.useMemo(() => getNumberParam("bytes", 512 * 1024), []);
	const count = React.useMemo(() => getNumberParam("count", 24), []);
	const [status, setStatus] = React.useState("idle");
	const [results, setResults] = React.useState<OPFSBenchmarkResult[] | null>(
		null,
	);

	React.useEffect(() => {
		if (!enabled) {
			return;
		}
		let cancelled = false;
		const run = async () => {
			setStatus("running");
			try {
				const clone = await runOPFSBenchmark({
					protocol: "clone",
					bytes,
					count,
				});
				const legacy = await runOPFSBenchmark({
					protocol: "legacy",
					bytes,
					count,
				});
				if (!cancelled) {
					setResults([clone, legacy]);
					setStatus("ready");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setStatus("error");
				}
			}
		};
		void run();
		return () => {
			cancelled = true;
		};
	}, [bytes, count, enabled]);

	if (!enabled) {
		return null;
	}

	return (
		<div>
			<div data-testid="opfs-benchmark-status">{status}</div>
			<pre data-testid="opfs-benchmark-results">
				{results ? JSON.stringify(results, null, 2) : ""}
			</pre>
		</div>
	);
};

type SQLiteBenchmarkResult = {
	protocol: SqliteWorkerProtocol;
	synchronous?: SQLiteSynchronousMode;
	payloadBytes: number;
	count: number;
	createMs: number;
	execMs: number;
	prepareMs: number;
	insertMs: number;
	selectMs: number;
	profile: SQLiteProfileSummary;
};

const runSQLiteBenchmark = async ({
	protocol,
	synchronous,
	bytes,
	count,
}: {
	protocol: SqliteWorkerProtocol;
	synchronous?: SQLiteSynchronousMode;
	bytes: number;
	count: number;
}): Promise<SQLiteBenchmarkResult> => {
	const { createDatabase } = (await import(
		"@peerbit/indexer-sqlite3"
	)) as unknown as {
		createDatabase: (
			directory?: string,
			options?: SQLiteCreateOptions,
		) => Promise<SQLiteDatabaseLike>;
	};
	const directory = `sqlite-bench-${protocol}-${Date.now()}-${Math.random()
		.toString(16)
		.slice(2)}`;
	const profiles: SQLiteProfileSample[] = [];
	const createStart = performance.now();
	const db = await createDatabase(directory, {
		protocol,
		pragmas: synchronous ? { synchronous } : undefined,
		profile: true,
		onProfile: (sample) => profiles.push(sample),
	});
	const createMs = performance.now() - createStart;
	const payloads = Array.from({ length: count }, (_, index) => ({
		id: `row-${index}`,
		payload: createBenchmarkPayload(bytes, index),
	}));

	try {
		const execStart = performance.now();
		await db.exec(
			"create table if not exists bench (id TEXT PRIMARY KEY, payload BLOB NOT NULL) strict",
		);
		const execMs = performance.now() - execStart;

		const prepareStart = performance.now();
		const insertStatement = await db.prepare(
			"insert or replace into bench (id, payload) values (?, ?)",
		);
		const selectStatement = await db.prepare(
			"select payload from bench where id = ? limit 1",
		);
		const prepareMs = performance.now() - prepareStart;

		const insertStart = performance.now();
		for (const payload of payloads) {
			await insertStatement.run([payload.id, payload.payload]);
		}
		const insertMs = performance.now() - insertStart;

		const selectStart = performance.now();
		for (const payload of payloads) {
			const row = await selectStatement.get([payload.id]);
			const value = row?.payload;
			if (!(value instanceof Uint8Array) || value.length !== bytes) {
				throw new Error(`Unexpected SQLite payload for ${payload.id}`);
			}
		}
		const selectMs = performance.now() - selectStart;

		await insertStatement.finalize?.();
		await selectStatement.finalize?.();

		return {
			protocol,
			synchronous,
			payloadBytes: bytes,
			count,
			createMs,
			execMs,
			prepareMs,
			insertMs,
			selectMs,
			profile: summarizeSQLiteProfiles(profiles),
		};
	} finally {
		await db.drop();
	}
};

const SQLiteBenchmarkStatus = () => {
	const enabled = React.useMemo(getSQLiteBenchmarkEnabled, []);
	const bytes = React.useMemo(() => getNumberParam("bytes", 256 * 1024), []);
	const count = React.useMemo(() => getNumberParam("count", 12), []);
	const synchronous = React.useMemo(getSQLiteSynchronous, []);
	const [status, setStatus] = React.useState("idle");
	const [results, setResults] = React.useState<SQLiteBenchmarkResult[] | null>(
		null,
	);

	React.useEffect(() => {
		if (!enabled) {
			return;
		}
		let cancelled = false;
		const run = async () => {
			setStatus("running");
			try {
				const legacy = await runSQLiteBenchmark({
					protocol: "legacy",
					synchronous,
					bytes,
					count,
				});
				const clone = await runSQLiteBenchmark({
					protocol: "clone",
					synchronous,
					bytes,
					count,
				});
				if (!cancelled) {
					setResults([legacy, clone]);
					setStatus("ready");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setStatus("error");
				}
			}
		};
		void run();
		return () => {
			cancelled = true;
		};
	}, [bytes, count, enabled, synchronous]);

	if (!enabled) {
		return null;
	}

	return (
		<div>
			<div data-testid="sqlite-benchmark-status">{status}</div>
			<pre data-testid="sqlite-benchmark-results">
				{results ? JSON.stringify(results, null, 2) : ""}
			</pre>
		</div>
	);
};

@variant(0)
class BenchDoc {
	@field({ type: "string" })
	id!: string;

	@field({ type: Uint8Array })
	bytes!: Uint8Array;

	constructor(props?: { id: string; bytes: Uint8Array }) {
		if (!props) return;
		this.id = props.id;
		this.bytes = props.bytes;
	}
}

@variant(0)
class BenchDocMetaIndex {
	@field({ type: "string" })
	id!: string;

	@field({ type: "u32" })
	size!: number;

	constructor(props?: { id: string; bytes: Uint8Array }) {
		if (!props) return;
		this.id = props.id;
		this.size = props.bytes.byteLength;
	}
}

type DocumentBenchmarkResult = {
	payloadBytes: number;
	count: number;
	inMemory: boolean;
	simpleIndex: boolean;
	docIndexMode: "full" | "meta";
	persisted: boolean | undefined;
	sqliteProtocol?: SqliteWorkerProtocol;
	sqliteSynchronous?: SQLiteSynchronousMode;
	serializeMs: number;
	blockPutMs: number;
	documentPutMs: number;
	sqliteProfile?: SQLiteProfileSummary;
};

const createBenchmarkPayload = (bytes: number, index: number) => {
	const value = new Uint8Array(bytes);
	for (let i = 0; i < value.length; i++) {
		value[i] = (i + index) % 251;
	}
	return value;
};

const DocumentBenchmarkStatus = ({
	sqliteProtocol,
	resetSQLiteProfiles,
	getSQLiteProfiles,
}: {
	sqliteProtocol?: SqliteWorkerProtocol;
	resetSQLiteProfiles: () => void;
	getSQLiteProfiles: () => SQLiteProfileSample[];
}) => {
	const { peer, persisted } = usePeer();
	const payloadBytes = React.useMemo(
		() => getNumberParam("bytes", 256 * 1024),
		[],
	);
	const count = React.useMemo(() => getNumberParam("count", 12), []);
	const sqliteSynchronous = React.useMemo(getSQLiteSynchronous, []);
	const inMemory = React.useMemo(getInMemoryEnabled, []);
	const simpleIndex = React.useMemo(getSimpleIndexEnabled, []);
	const docIndexMode = React.useMemo(getDocumentIndexMode, []);
	const docs = React.useMemo(
		() =>
			docIndexMode === "meta"
				? new Documents<BenchDoc, BenchDocMetaIndex>()
				: new Documents<BenchDoc>(),
		[docIndexMode],
	);
	const { program, loading } = useProgram(peer, peer ? docs : undefined, {
		args: {
			type: BenchDoc,
			index:
				docIndexMode === "meta"
					? {
							type: BenchDocMetaIndex,
						}
					: undefined,
			replicate: false,
		},
		keepOpenOnUnmount: true,
	});
	const hasRunRef = React.useRef(false);
	const [status, setStatus] = React.useState("idle");
	const [result, setResult] = React.useState<DocumentBenchmarkResult | null>(
		null,
	);

	React.useEffect(() => {
		if (!peer || !program || loading || hasRunRef.current) {
			return;
		}
		hasRunRef.current = true;
		let cancelled = false;
			const run = async () => {
				setStatus("running");
				try {
					resetSQLiteProfiles();
					const samples = Array.from({ length: count }, (_, i) => ({
						id: `bench-${i}`,
						bytes: createBenchmarkPayload(payloadBytes, i),
					}));

				const serializeStart = performance.now();
				for (const sample of samples) {
					serialize(new BenchDoc(sample));
				}
				const serializeMs = performance.now() - serializeStart;

				const blocks = (peer as any).services?.blocks as
					| { put: (value: Uint8Array) => Promise<string> }
					| undefined;
				if (!blocks) {
					throw new Error("Peer blocks service missing");
				}

				const blockPutStart = performance.now();
				for (const sample of samples) {
					await blocks.put(sample.bytes);
				}
				const blockPutMs = performance.now() - blockPutStart;

				const documentPutStart = performance.now();
				for (const sample of samples) {
					await program.put(new BenchDoc(sample), {
						replicate: false,
						target: "none",
						replicas: 1,
					});
				}
				const documentPutMs = performance.now() - documentPutStart;

				if (!cancelled) {
					const sqliteProfiles = getSQLiteProfiles();
					setResult({
						payloadBytes,
						count,
						inMemory,
						simpleIndex,
						docIndexMode,
						persisted,
						sqliteProtocol,
						sqliteSynchronous,
						serializeMs,
						blockPutMs,
						documentPutMs,
						sqliteProfile:
							sqliteProfiles.length > 0
								? summarizeSQLiteProfiles(sqliteProfiles)
								: undefined,
					});
					setStatus("ready");
				}
			} catch (error) {
				console.error(error);
				if (!cancelled) {
					setStatus("error");
				}
			}
		};
		void run();
		return () => {
			cancelled = true;
		};
	}, [
		count,
		getSQLiteProfiles,
		inMemory,
		loading,
		payloadBytes,
		peer,
		persisted,
		program,
		resetSQLiteProfiles,
		simpleIndex,
		docIndexMode,
		sqliteSynchronous,
		sqliteProtocol,
	]);

	return (
		<div>
			<div data-testid="document-benchmark-status">{status}</div>
			<pre data-testid="document-benchmark-results">
				{result ? JSON.stringify(result, null, 2) : ""}
			</pre>
		</div>
	);
};

const PeerInfo = () => {
	const { peer, loading, status, error } = usePeer();
	const [peerHash, setPeerHash] = React.useState<string | undefined>(undefined);

	React.useEffect(() => {
		if (peer?.identity?.publicKey?.hashcode) {
			setPeerHash(peer.identity.publicKey.hashcode());
		}
	}, [peer]);

	return (
		<div>
			<h1>Peerbit React E2E</h1>
			<SqliteStatus />
			<DocumentQueryStatus />
			<div data-testid="status">status: {status}</div>
			<div data-testid="loading">loading: {loading ? "yes" : "no"}</div>
			<div data-testid="peer-hash">{peerHash ?? "no-peer"}</div>
			{error ? <div data-testid="error">{error.message}</div> : null}
		</div>
	);
};

const App = () => {
	const bootstrapAddrs = React.useMemo(getBootstrapAddrs, []);
	const canonical = React.useMemo(getCanonicalEnabled, []);
	const opfsBenchmark = React.useMemo(getOPFSBenchmarkEnabled, []);
	const sqliteBenchmark = React.useMemo(getSQLiteBenchmarkEnabled, []);
	const documentBenchmark = React.useMemo(getDocumentBenchmarkEnabled, []);
	const inMemory = React.useMemo(getInMemoryEnabled, []);
	const simpleIndex = React.useMemo(getSimpleIndexEnabled, []);
	const sqliteProtocol = React.useMemo(getSQLiteProtocol, []);
	const sqliteSynchronous = React.useMemo(getSQLiteSynchronous, []);
	const sqliteProfilesRef = React.useRef<SQLiteProfileSample[]>([]);

	const resetSQLiteProfiles = React.useCallback(() => {
		sqliteProfilesRef.current = [];
		(window as any).__sqliteProfiles = sqliteProfilesRef.current;
	}, []);

	const getSQLiteProfiles = React.useCallback(
		() => sqliteProfilesRef.current.slice(),
		[],
	);

	if (opfsBenchmark) {
		return <OPFSBenchmarkStatus />;
	}

	if (sqliteBenchmark) {
		return <SQLiteBenchmarkStatus />;
	}

	const network = React.useMemo(() => {
		return bootstrapAddrs.length
			? { type: "explicit" as const, bootstrap: bootstrapAddrs }
			: ("local" as const);
	}, [bootstrapAddrs]);

	return (
		<PeerProvider
			config={
				canonical
					? {
							runtime: "canonical",
							transport: {
								kind: "service-worker",
								options: { url: "/service-worker.js", type: "module" },
							},
							open: { adapters: [documentAdapter] },
						}
						: {
								runtime: "node",
								network,
								inMemory,
								indexer: simpleIndex
									? async () => {
											const { create } = await import("@peerbit/indexer-simple");
											return create();
										}
										: sqliteProtocol
											? async (directory?: string) => {
													const { create } = (await import(
														"@peerbit/indexer-sqlite3"
													)) as unknown as {
													create: (
														directory?: string,
														options?: SQLiteCreateOptions,
													) => Promise<any>;
												};
												return create(directory, {
													protocol: sqliteProtocol,
													pragmas: sqliteSynchronous
														? { synchronous: sqliteSynchronous }
														: undefined,
													profile: documentBenchmark,
													onProfile: (sample: SQLiteProfileSample) => {
														sqliteProfilesRef.current.push(sample);
														(window as any).__sqliteProfiles =
															sqliteProfilesRef.current;
													},
												});
											}
									: undefined,
						  }
			}
		>
			{documentBenchmark ? (
				<DocumentBenchmarkStatus
					sqliteProtocol={sqliteProtocol}
					resetSQLiteProfiles={resetSQLiteProfiles}
					getSQLiteProfiles={getSQLiteProfiles}
				/>
			) : (
				<PeerInfo />
			)}
		</PeerProvider>
	);
};

export default App;
