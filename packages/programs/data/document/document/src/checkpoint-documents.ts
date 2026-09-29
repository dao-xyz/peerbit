import { field, fixedArray, serialize, variant, vec } from "@dao-xyz/borsh";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import { calculateRawCid } from "@peerbit/blocks-interface";
import {
	Ed25519PublicKey,
	type Identity,
	randomBytes,
	sha256Base64Sync,
	sha256Sync,
	verify,
} from "@peerbit/crypto";
import { Context } from "@peerbit/document-interface";
import { toId } from "@peerbit/indexer-interface";
import {
	type Change,
	Entry,
	EntryType,
	Timestamp,
	scanCanonicalPublicEntryV0,
} from "@peerbit/log";
import {
	Program,
	type ProgramEvents,
	TerminalOperationNotStartedError,
} from "@peerbit/program";
import { type Args, SharedLog } from "@peerbit/shared-log";
import { concat, equals, toString } from "uint8arrays";
import {
	type CheckpointApproval,
	createCheckpointCertificate,
	readCheckpointCertificate,
	signCheckpointApproval,
} from "./checkpoint-certificate.js";
import {
	CHECKPOINT_MAX_OPERATION_BYTES,
	CheckpointDocument,
	type CheckpointDocumentValue,
	CheckpointOperation,
	assertCheckpointKey,
	captureCheckpointBytes,
	decodeCheckpointOperation,
} from "./checkpoint-operation.js";
import {
	type CheckpointSnapshot,
	createCheckpointFreeze,
	createCheckpointSnapshot,
	readCheckpointFreeze,
	readCheckpointSnapshot,
} from "./checkpoint-snapshot.js";
import type { DocumentEvents } from "./events.js";
import { BORSH_ENCODING_OPERATION, type Operation } from "./operation.js";
import { DocumentIndex, type OpenOptions } from "./search.js";

const encoder = new TextEncoder();
const MAX_ENTRY_BYTES = CHECKPOINT_MAX_OPERATION_BYTES + 32 * 1024;
const MAX_PARENTS = 32;
function fail(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}
type Index = DocumentIndex<CheckpointDocument, CheckpointDocument, any>;
type RecoveryLog = {
	openWithLocalRecovery(
		options: Args<Operation, any, any>,
		recover: () => Promise<void | "local-only">,
	): Promise<void>;
};
type RecoveryIndex = {
	openForDocuments(
		options: OpenOptions<CheckpointDocument, CheckpointDocument, any>,
	): Promise<void>;
};
type Fact = {
	cid: string;
	bytes: Uint8Array;
	entry: Entry<Operation>;
	operation: CheckpointOperation;
	document?: CheckpointDocument;
	created: bigint;
};
type Floor = {
	cid: string;
	epoch: string;
	phase: "active" | "frozen" | "forked";
	freeze?: string;
	collection?: string[];
	proposal?: string;
};

export type CheckpointDocumentsOpenOptions = {
	/** An externally authenticated current checkpoint. Omit only for local offline reopen. */
	checkpoint?: string;
};

/**
 * Versioned, public-value document resource. Checkpoints are mandatory, not a
 * legacy Documents mode. Every epoch has its own retained APPEND log; ordinary
 * deletes are logical tombstones. Source blocks are never physically retired.
 */
@variant("checkpoint_documents_v1")
export class CheckpointDocuments extends Program<
	CheckpointDocumentsOpenOptions,
	DocumentEvents<CheckpointDocument, CheckpointDocument> & ProgramEvents
> {
	@field({ type: fixedArray("u8", 32) })
	private nonce: Uint8Array;

	@field({ type: Ed25519PublicKey })
	private owner: Ed25519PublicKey;

	@field({ type: vec(Ed25519PublicKey) })
	private writers: Ed25519PublicKey[];

	private resource!: Uint8Array;
	private checkpoint!: CheckpointSnapshot;
	private checkpointCid!: string;
	private checkpointDigest!: Uint8Array;
	private journal!: CrashSafeTwoSlotCheckpoint;
	private shared!: SharedLog<Operation, any, any>;
	private projection!: Index;
	private ready = false;
	private fault?: unknown;
	private pending: Promise<unknown> = Promise.resolve();
	private projectionPending: Promise<void> = Promise.resolve();
	private checkpointTransition = false;
	private session = 0;
	private opening = false;
	private facts = new Map<string, Fact>();
	private validated = new Map<string, Fact>();
	private frontier = new Map<string, Map<string, Fact>>();
	private base = new Map<string, Map<string, Fact>>();
	private decoded = new WeakMap<Entry<Operation>, Promise<Fact>>();

	constructor(properties: {
		owner: Ed25519PublicKey;
		writers?: Ed25519PublicKey[];
		id?: Uint8Array;
	}) {
		super();
		this.nonce = captureCheckpointBytes(properties.id ?? randomBytes(32), 32);
		this.owner = new Ed25519PublicKey({
			publicKey: captureCheckpointBytes(properties.owner.publicKey, 32),
		});
		this.writers = (properties.writers ?? [properties.owner]).map(
			(key) =>
				new Ed25519PublicKey({
					publicKey: captureCheckpointBytes(key.publicKey, 32),
				}),
		);
		this.writers.sort((a, b) =>
			toString(a.publicKey, "hex").localeCompare(toString(b.publicKey, "hex")),
		);
		this.descriptor();
	}

	private descriptor(): Uint8Array {
		fail(
			this.nonce.length === 32 &&
				this.writers.length > 0 &&
				this.writers.length <= 32,
			"Invalid checkpoint resource descriptor",
		);
		const keys = this.writers.map((key) => toString(key.publicKey, "hex"));
		fail(
			keys.every(
				(key, i) => key.length === 64 && (i === 0 || keys[i - 1]! < key),
			),
			"Checkpoint writers must be sorted and unique",
		);
		fail(
			this.writers.some((key) => key.equals(this.owner)),
			"Checkpoint owner must be a writer",
		);
		return sha256Sync(
			concat([
				encoder.encode("peerbit:checkpoint-documents:resource:v1\0"),
				serialize(this),
			]),
		);
	}

	/** Creates an explicit genesis anchor; this does not assert that genesis is still current. */
	async createGenesis(
		blocks: Parameters<typeof createCheckpointSnapshot>[0]["blocks"],
		identity: Identity<Ed25519PublicKey>,
	): Promise<string> {
		fail(
			identity.publicKey.equals(this.owner),
			"Only the owner can create genesis",
		);
		const resource = this.descriptor();
		const snapshot = await createCheckpointSnapshot({
			blocks,
			resource,
			owner: identity,
			epoch: 0n,
			previous: null,
			frontier: [],
		});
		return (
			await createCheckpointCertificate({
				blocks,
				resource,
				proposal: snapshot.cid,
				writers: this.writers.map((writer) => writer.publicKey),
				approvals: [],
				genesis: true,
			})
		).cid;
	}

	private readFloor(): Floor | undefined {
		const current = this.journal.current;
		if (!current) return undefined;
		const floor = JSON.parse(
			new TextDecoder("utf-8", { fatal: true }).decode(current.payload),
		) as Floor;
		fail(
			typeof floor.cid === "string" &&
				/^(0|[1-9][0-9]*)$/.test(floor.epoch) &&
				["active", "frozen", "forked"].includes(floor.phase),
			"Invalid checkpoint floor",
		);
		return floor;
	}

	private async writeFloor(floor: Floor): Promise<void> {
		await this.journal.commit(encoder.encode(JSON.stringify(floor)));
	}

	async open(options: CheckpointDocumentsOpenOptions = {}): Promise<void> {
		fail(
			options && Object.keys(options).every((key) => key === "checkpoint"),
			"Unsupported Checkpoint Documents open option",
		);
		fail(
			!this.opening && !this.checkpointTransition && !this.ready,
			"Checkpoint Documents is already active",
		);
		this.opening = true;
		try {
			await this.openInternal(options);
		} finally {
			this.opening = false;
		}
	}

	private async openInternal(
		options: CheckpointDocumentsOpenOptions,
		reconcile?: () => Promise<void>,
	): Promise<void> {
		this.session = (this.session ?? 0) + 1;
		this.ready = false;
		this.fault = undefined;
		this.pending = Promise.resolve();
		this.projectionPending = Promise.resolve();
		this.facts = new Map();
		this.validated = new Map();
		this.frontier = new Map();
		this.base = new Map();
		this.decoded = new WeakMap();
		this.resource = this.descriptor();
		fail(
			this.node.services.blocks.crashSafeDurability,
			"Checkpoint Documents requires crash-safe block storage",
		);
		const store = await this.node.storage.sublevel(
			`checkpoint-documents/${toString(this.resource, "hex")}`,
		);
		await store.open();
		this.journal = await CrashSafeTwoSlotCheckpoint.open({
			store,
			scope: this.resource,
			maxPayloadBytes: 4096,
		});
		const previous = this.readFloor();
		const recoverFrozen =
			previous?.phase === "frozen" &&
			(!options.checkpoint || options.checkpoint === previous.cid);
		fail(!reconcile || recoverFrozen, "Reconciliation requires a frozen epoch");
		fail(
			previous?.phase !== "forked",
			"Checkpoint authority fork requires explicit recovery",
		);
		const cid = options.checkpoint ?? previous?.cid;
		fail(cid, "A trusted checkpoint anchor is required for first open");
		const certificate = await readCheckpointCertificate({
			blocks: this.node.services.blocks,
			cid,
			resource: this.resource,
			writers: this.writers.map((writer) => writer.publicKey),
			remote: !!options.checkpoint,
		});
		const snapshot = await readCheckpointSnapshot({
			blocks: this.node.services.blocks,
			cid: certificate.proposal,
			resource: this.resource,
			owner: this.owner,
			remote: !!options.checkpoint,
		});
		fail(
			certificate.genesis === (snapshot.epoch === 0n),
			"Invalid genesis certificate",
		);
		if (previous && previous.cid !== cid) {
			if (BigInt(previous.epoch) === snapshot.epoch) {
				await this.writeFloor({ ...previous, phase: "forked" });
				throw new Error("Conflicting checkpoint at the same epoch");
			}
			fail(
				snapshot.epoch > BigInt(previous.epoch),
				"Checkpoint rollback rejected",
			);
			const localWriter = this.writers.some((writer) =>
				writer.equals(this.node.identity.publicKey),
			);
			fail(
				snapshot.previous === previous.cid &&
					snapshot.epoch === BigInt(previous.epoch) + 1n,
				"Checkpoint advance must be a direct successor",
			);
			fail(
				!localWriter ||
					(previous.phase === "frozen" &&
						previous.proposal === certificate.proposal),
				"Checkpoint advance requires this writer's frozen frontier approval",
			);
		}
		this.checkpoint = snapshot;
		this.checkpointCid = cid;
		this.checkpointDigest = sha256Sync(encoder.encode(cid));
		for await (const record of snapshot.frontier()) {
			const bytes = await this.node.services.blocks.get(record.cid, {
				remote: !!options.checkpoint,
			});
			fail(bytes, "Missing checkpoint boundary entry");
			const fact = await this.authenticate(bytes, record.cid);
			fail(
				fact.operation.epoch < snapshot.epoch &&
					record.created <= fact.entry.meta.clock.timestamp.wallTime,
				"Invalid checkpoint boundary epoch/context",
			);
			fact.created = record.created;
			const tips = this.base.get(fact.operation.key) ?? new Map<string, Fact>();
			tips.set(fact.cid, fact);
			this.base.set(fact.operation.key, tips);
			await this.node.services.blocks.put(fact.bytes);
		}
		for (const tips of this.base.values())
			fail(
				[...tips.values()].some((tip) => tip.operation.kind === 0),
				"Checkpoint must omit tombstone-only keys",
			);
		await snapshot.retain();
		await certificate.retain();
		await this.node.services.blocks.crashSafeDurability.barrier();
		if (!previous || previous.cid !== cid)
			await this.writeFloor({
				cid,
				epoch: snapshot.epoch.toString(),
				phase: "active",
			});

		const logId = sha256Sync(concat([this.resource, this.checkpointDigest]));
		this.shared = new SharedLog({ id: logId });
		this.projection = new DocumentIndex();
		await (this.shared as Program).beforeOpen(this.node, { parent: this });
		await (this.projection as Program).beforeOpen(this.node, { parent: this });
		await (this.projection as unknown as RecoveryIndex).openForDocuments({
			documentEvents: this.events,
			documentType: CheckpointDocument,
			dbType: CheckpointDocuments,
			log: this.shared,
			indexBy: "id",
			cache: { resolver: 0 },
			canRead: () => this.ready && !this.fault,
			canSearch: () => this.ready && !this.fault,
			maybeOpen: async () => {
				throw new Error(
					"Program values are not supported by this checkpoint profile",
				);
			},
			replicate: async () => {
				throw new Error(
					"Checkpoint query responses are not admission evidence",
				);
			},
		});
		await (this.shared as unknown as RecoveryLog).openWithLocalRecovery(
			{
				encoding: BORSH_ENCODING_OPERATION,
				replicate: { factor: 1 },
				keep: () => true,
				nativeGraph: false,
				nativeBackbone: false,
				nativeRangePlanner: false,
				appendDurability: "strict",
				canJoin: async (entry) => {
					try {
						this.assertEpoch(await this.decode(entry));
						return true;
					} catch {
						return false;
					}
				},
				canAppend: async (entry) => {
					try {
						await this.validate(await this.decode(entry));
						return true;
					} catch {
						return false;
					}
				},
				onChange: (change) => this.change(change),
			},
			async () => {
				fail(
					this.shared.log.blocks.crashSafeDurability,
					"Checkpoint Documents requires crash-safe suffix blocks",
				);
				fail(
					this.shared.log.entryIndex.properties.index.crashSafeDurability,
					"Checkpoint Documents requires crash-safe log index storage",
				);
				await this.projection.index.del({ query: [] });
				for (const [key, tips] of this.base) {
					this.frontier.set(key, new Map(tips));
					await this.project(key);
				}
				// Only this epoch is enumerated. Old source logs are retained but never scanned.
				const iterator = this.shared.log.entryIndex.iterate(
					[],
					this.shared.log.sortFn.sort,
					true,
				);
				try {
					while (!iterator.done()) {
						for (const entry of await iterator.next(128)) {
							const fact = await this.decode(entry);
							await this.validate(fact);
							await this.accept(fact, false);
						}
					}
				} finally {
					await iterator.close();
				}
				await reconcile?.();
				for (const tips of this.frontier.values())
					for (const fact of tips.values())
						this.shared.log.hlc.update(fact.entry.meta.clock.timestamp);
				if (recoverFrozen) return "local-only";
			},
		);
		if (recoverFrozen) {
			await this.shared.close(this);
			await this.projection.close(this);
			return;
		}
		this.ready = true;
		// These runtime children deliberately are not serialized descriptor fields.
		// Only replication needs activation: this profile queries its local index
		// and deliberately leaves the generic unauthenticated query RPC unopened.
		await this.shared.afterOpen();
	}

	private assertReady(): void {
		fail(
			this.ready && !this.closed && !this.fault,
			"Checkpoint Documents is not ready",
		);
	}

	/** Read-only query surface: mutable lower log/index handles are deliberately not exposed. */
	get index(): Pick<Index, "get" | "search" | "iterate" | "getSize"> {
		this.assertReady();
		const session = this.session;
		const projection = this.projection;
		const assertCurrent = () => {
			this.assertReady();
			fail(
				session === this.session,
				"Query handle belongs to a closed checkpoint session",
			);
		};
		const checkedResult = (result: any) => {
			if (result && typeof result.then === "function")
				return result.then((value: unknown) => {
					assertCurrent();
					return value;
				});
			assertCurrent();
			return result;
		};
		const guarded = <F extends (...args: any[]) => any>(method: F): F =>
			((...args: Parameters<F>) => {
				assertCurrent();
				const options = args[1];
				fail(
					options?.remote == null || options.remote === false,
					"Checkpoint queries require locally admitted state; remote queries are unsupported",
				);
				fail(
					options?.local !== false,
					"Checkpoint queries require locally admitted state",
				);
				args[1] = { ...options, local: true, remote: false };
				const result = method.apply(projection, args);
				if (result && typeof result[Symbol.asyncIterator] === "function") {
					const invoke =
						(name: string) =>
						(...values: unknown[]) => {
							assertCurrent();
							return checkedResult(result[name](...values));
						};
					return {
						next: invoke("next"),
						all: invoke("all"),
						first: invoke("first"),
						done: invoke("done"),
						pending: invoke("pending"),
						close: () => result.close(),
						async *[Symbol.asyncIterator]() {
							assertCurrent();
							for await (const value of result) {
								assertCurrent();
								yield value;
							}
						},
					};
				}
				return checkedResult(result);
			}) as F;
		return {
			get: guarded(this.projection.get),
			search: guarded(this.projection.search),
			iterate: guarded(this.projection.iterate),
			getSize: guarded(this.projection.getSize),
		};
	}

	get currentCheckpoint(): string {
		return this.checkpointCid;
	}

	get status(): "ready" | "frozen" | "unavailable" {
		return this.ready && !this.closed && !this.fault
			? "ready"
			: this.journal && this.readFloor()?.phase === "frozen"
				? "frozen"
				: "unavailable";
	}

	private decode(entry: Entry<Operation>): Promise<Fact> {
		let fact = this.decoded.get(entry);
		if (!fact) {
			fact = this.authenticate(entry.getStorageBytes(), entry.hash, true);
			this.decoded.set(entry, fact);
		}
		return fact;
	}

	private async authenticate(
		raw: Uint8Array,
		expectedCid?: string,
		materialized = false,
	): Promise<Fact> {
		const bytes = captureCheckpointBytes(raw, MAX_ENTRY_BYTES);
		const scanned = scanCanonicalPublicEntryV0(bytes, {
			label: "Checkpoint document",
			minimumSignatures: 1,
			maximumSignatures: 1,
			maximumDirectParents: MAX_PARENTS,
			maximumMetadataBytes: 16 * 1024,
		});
		if (scanned.hasHash && materialized) {
			// Log index/wire materializations carry their hash; the canonical block
			// does not. Normalize only the bounded owned copy, never the candidate.
			scanned.entry.hash = undefined as unknown as string;
			return this.authenticate(serialize(scanned.entry), expectedCid);
		}
		fail(
			!scanned.hasHash && scanned.reservedBytes.every((byte) => byte === 0),
			"Invalid checkpoint entry framing",
		);
		fail(
			scanned.meta.type === EntryType.APPEND,
			"Checkpoint entries must retain APPEND evidence",
		);
		const operation = decodeCheckpointOperation(scanned.payloadBytes);
		fail(
			equals(operation.resource, this.resource),
			"Foreign checkpoint resource",
		);
		fail(
			scanned.meta.gid ===
				sha256Base64Sync(sha256Sync(encoder.encode(operation.key))),
			"Foreign checkpoint key graph",
		);
		const signer = scanned.signatures[0]!;
		fail(
			this.writers.some((writer) => writer.equals(signer.publicKey)) &&
				(await verify(signer, scanned.signableBytes)),
			"Unauthorized checkpoint entry",
		);
		const cid = (await calculateRawCid(bytes)).cid;
		fail(!expectedCid || expectedCid === cid, "Checkpoint entry CID mismatch");
		const document =
			operation.kind === 0
				? CheckpointDocument.fromBytes(operation.key, operation.data)
				: undefined;
		const entry = scanned.entry as unknown as Entry<Operation>;
		entry.hash = cid;
		return {
			cid,
			bytes,
			entry,
			operation,
			document,
			created: scanned.meta.clock.timestamp.wallTime,
		};
	}

	private assertEpoch(fact: Fact): void {
		fail(
			!this.fault &&
				fact.operation.epoch === this.checkpoint.epoch &&
				equals(fact.operation.checkpoint, this.checkpointDigest),
			"Stale or foreign checkpoint epoch",
		);
	}

	private async validate(fact: Fact): Promise<void> {
		this.assertEpoch(fact);
		const parents = fact.entry.meta.next;
		fail(
			new Set(parents).size === parents.length,
			"Duplicate checkpoint parents",
		);
		const prior: Fact[] = [];
		for (const cid of parents) {
			const parent = this.facts.get(cid) ?? this.validated.get(cid);
			fail(
				parent &&
					parent.operation.key === fact.operation.key &&
					parent.entry.meta.gid === fact.entry.meta.gid,
				"Missing or foreign admitted parent",
			);
			fail(await this.shared.log.has(cid), "Uncommitted checkpoint parent");
			fail(
				Timestamp.compare(
					parent.entry.meta.clock.timestamp,
					fact.entry.meta.clock.timestamp,
				) < 0,
				"Non-causal checkpoint timestamp",
			);
			prior.push(parent);
		}
		if (!parents.length) {
			prior.push(...(this.base.get(fact.operation.key)?.values() ?? []));
			for (const parent of prior) {
				fail(
					Timestamp.compare(
						parent.entry.meta.clock.timestamp,
						fact.entry.meta.clock.timestamp,
					) < 0,
					"Non-causal checkpoint boundary timestamp",
				);
			}
		}
		// A recreation after complete deletion starts a new document lifetime.
		// This is identical before and after the deleted-only key leaves a seal.
		const livePrior =
			fact.operation.kind === 0
				? prior.filter((parent) => parent.operation.kind === 0)
				: prior;
		fact.created = livePrior.length
			? livePrior.reduce(
					(minimum, item) => (item.created < minimum ? item.created : minimum),
					livePrior[0]!.created,
				)
			: fact.entry.meta.clock.timestamp.wallTime;
		fail(
			fact.created <= fact.entry.meta.clock.timestamp.wallTime,
			"Checkpoint timestamp precedes creation",
		);
		this.validated.set(fact.cid, fact);
	}

	private change(change: Change<Operation>): Promise<void> {
		const apply = async () => {
			fail(
				change.removed.length === 0,
				"Checkpoint suffix evidence must not be removed",
			);
			const added = await Promise.all(
				change.added.map(({ entry }) => this.decode(entry)),
			);
			// Received operations cross the same physical barriers as local writes
			// before becoming queryable or generating document change events.
			await this.shared.log.blocks.crashSafeDurability!.barrier();
			await this.shared.log.entryIndex.properties.index.crashSafeDurability!.barrier();
			added.sort((a, b) =>
				Timestamp.compare(
					a.entry.meta.clock.timestamp,
					b.entry.meta.clock.timestamp,
				),
			);
			for (const fact of added) {
				await this.validate(fact);
				await this.accept(fact, this.ready);
			}
		};
		const result = this.projectionPending.then(apply);
		this.projectionPending = result.catch((error) => {
			this.fault = error;
			this.ready = false;
		});
		return result;
	}

	private async accept(fact: Fact, events: boolean): Promise<void> {
		if (this.facts.has(fact.cid)) return;
		this.facts.set(fact.cid, fact);
		const key = fact.operation.key;
		const tips = this.frontier.get(key) ?? new Map<string, Fact>();
		if (fact.entry.meta.next.length === 0)
			for (const cid of this.base.get(key)?.keys() ?? []) tips.delete(cid);
		for (const cid of fact.entry.meta.next) tips.delete(cid);
		tips.set(fact.cid, fact);
		this.frontier.set(key, tips);
		await this.project(key, events);
	}

	private async project(key: string, events = false): Promise<void> {
		const tips = [...(this.frontier.get(key)?.values() ?? [])];
		tips.sort(
			(a, b) =>
				Timestamp.compare(
					a.entry.meta.clock.timestamp,
					b.entry.meta.clock.timestamp,
				) || (a.cid < b.cid ? -1 : a.cid === b.cid ? 0 : 1),
		);
		const winner = tips.at(-1);
		const prior = events
			? await this.projection.index.get(toId(key))
			: undefined;
		if (!winner?.document) {
			await this.projection.delMany([toId(key)]);
			if (events && prior)
				this.events.dispatchEvent(
					new CustomEvent("change", {
						detail: { added: [], removed: [prior.value] },
					}),
				);
			return;
		}
		const context = new Context({
			created: winner.created,
			modified: winner.entry.meta.clock.timestamp.wallTime,
			head: winner.cid,
			gid: winner.entry.meta.gid,
			size: winner.operation.data.length,
		});
		const document = CheckpointDocument.fromBytes(key, winner.operation.data);
		await this.projection.putWithContext(document, toId(key), context);
		if (events)
			this.events.dispatchEvent(
				new CustomEvent("change", {
					detail: { added: [document], removed: prior ? [prior.value] : [] },
				}),
			);
	}

	put(value: CheckpointDocumentValue): Promise<string> {
		fail(
			arguments.length === 1,
			"Checkpoint put does not accept delivery options",
		);
		const document = CheckpointDocument.from(value);
		return this.write(0, document.id, document.value);
	}

	del(key: string): Promise<string> {
		fail(
			arguments.length === 1,
			"Checkpoint del does not accept delivery options",
		);
		assertCheckpointKey(key);
		return this.write(1, key, new Uint8Array());
	}

	private write(kind: 0 | 1, key: string, data: Uint8Array): Promise<string> {
		this.assertReady();
		fail(
			this.writers.some((writer) =>
				writer.equals(this.node.identity.publicKey),
			),
			"Local identity is not a writer",
		);
		const work = this.pending.then(async () => {
			fail(!this.fault, "Checkpoint Documents is faulted");
			const tips = [...(this.frontier.get(key)?.values() ?? [])]
				.filter((fact) => fact.operation.epoch === this.checkpoint.epoch)
				.sort((a, b) => (a.cid < b.cid ? -1 : a.cid === b.cid ? 0 : 1));
			const parents = tips.slice(0, MAX_PARENTS);
			if (kind === 0) {
				const earliest = tips.reduce<Fact | undefined>(
					(earliest, tip) =>
						tip.operation.kind === 0 &&
						(!earliest || tip.created < earliest.created)
							? tip
							: earliest,
					undefined,
				);
				// Even a wide mixed frontier must preserve a live branch's creation
				// context, not accidentally select only deletes and reset its age.
				if (earliest && !parents.includes(earliest)) {
					parents[parents.length - 1] = earliest;
					parents.sort((a, b) => (a.cid < b.cid ? -1 : 1));
				}
			}
			const next = parents.map((fact) => fact.entry);
			// Bound each signed operation, not admission by arrival-order. Large
			// concurrent frontiers remain valid and merge incrementally over writes.
			const operation = new CheckpointOperation({
				resource: this.resource,
				epoch: this.checkpoint.epoch,
				checkpoint: this.checkpointDigest,
				kind,
				key,
				data,
			});
			const { entry } = await this.shared.append(operation, {
				meta: {
					next,
					type: EntryType.APPEND,
					gidSeed: sha256Sync(encoder.encode(key)),
				},
				durability: "strict",
			});
			return entry.hash;
		});
		this.pending = work.catch((error) => {
			this.fault = error;
			this.ready = false;
		});
		return work;
	}

	private async freeze(): Promise<void> {
		fail(
			(this.parents?.length ?? 0) <= 1,
			"Checkpoint sealing requires exclusive program ownership",
		);
		if (!this.ready && this.readFloor()?.phase === "frozen") {
			fail(
				(await this.close(this.parents?.[0])) && this.closed,
				"Checkpoint writer did not close",
			);
			return;
		}
		this.assertReady();
		this.ready = false;
		await this.pending;
		fail(!this.fault, "Cannot freeze a faulted checkpoint resource");
		fail(
			(await this.close(this.parents?.[0])) && this.closed,
			"Checkpoint writer did not close",
		);
		await this.projectionPending;
		fail(!this.fault, "Cannot freeze a faulted checkpoint projection");
		await this.writeFloor({
			cid: this.checkpointCid,
			epoch: this.checkpoint.epoch.toString(),
			phase: "frozen",
		});
	}

	private retainedFrontier(): Fact[] {
		return [...this.frontier.values()]
			.filter((tips) =>
				[...tips.values()].some((tip) => tip.operation.kind === 0),
			)
			.flatMap((tips) => [...tips.values()])
			.sort((a, b) => (a.cid < b.cid ? -1 : a.cid === b.cid ? 0 : 1));
	}

	/** Freeze this writer durably and publish its immutable active-epoch frontier. */
	async freezeCheckpoint(): Promise<string> {
		return this.transition(() => this.freezeCheckpointClosed());
	}

	private async freezeCheckpointClosed(): Promise<string> {
		fail(
			this.writers.some((writer) =>
				writer.equals(this.node.identity.publicKey),
			),
			"Only fixed writers freeze checkpoints",
		);
		await this.freeze();
		const floor = this.readFloor()!;
		if (floor.freeze) return floor.freeze;
		// The resource RPC is closed. Export the complete active causal closure
		// through the still-live node block service, not only terminal entries.
		for (const fact of this.facts.values())
			await this.node.services.blocks.put(fact.bytes);
		const tips = [...this.frontier.values()]
			.flatMap((tips) => [...tips.values()])
			.filter((tip) => tip.operation.epoch === this.checkpoint.epoch)
			.sort((a, b) => (a.cid < b.cid ? -1 : 1));
		const manifest = await createCheckpointFreeze({
			blocks: this.node.services.blocks,
			resource: this.resource,
			owner: this.node.identity,
			epoch: this.checkpoint.epoch + 1n,
			previous: this.checkpointCid,
			frontier: tips.map((tip) => ({ cid: tip.cid, created: tip.created })),
		});
		await this.node.services.blocks.crashSafeDurability!.barrier();
		await this.writeFloor({ ...floor, freeze: manifest.cid });
		return manifest.cid;
	}

	/** Propose the reconciled frontier after collecting every fixed writer's freeze. */
	async prepareCheckpoint(freezes?: readonly string[]): Promise<string> {
		const captured = freezes ? [...freezes] : undefined;
		return this.transition(() => this.prepareCheckpointClosed(captured));
	}

	private async transition<T>(work: () => Promise<T>): Promise<T> {
		fail(
			!this.checkpointTransition,
			"A checkpoint transition is already in progress",
		);
		this.checkpointTransition = true;
		try {
			return await work();
		} finally {
			this.checkpointTransition = false;
		}
	}

	private async prepareCheckpointClosed(
		freezes?: readonly string[],
	): Promise<string> {
		fail(
			this.node.identity.publicKey.equals(this.owner),
			"Only the owner can propose a checkpoint",
		);
		fail(
			freezes || this.writers.length === 1,
			"Collect every writer's freeze before preparing a checkpoint",
		);
		if (freezes)
			fail(
				freezes.length === this.writers.length &&
					new Set(freezes).size === freezes.length &&
					freezes.every(
						(cid) =>
							typeof cid === "string" && cid.length > 0 && cid.length <= 128,
					),
				"Checkpoint requires one freeze from every writer",
			);
		const ownFreeze = await this.freezeCheckpointClosed();
		const collection = await this.reconcileFrozen(freezes ?? [ownFreeze]);
		const previousProposal = this.readFloor()?.proposal;
		if (previousProposal) return previousProposal;
		const tips = this.retainedFrontier();
		for (const tip of tips) await this.node.services.blocks.put(tip.bytes);
		const snapshot = await createCheckpointSnapshot({
			blocks: this.node.services.blocks,
			resource: this.resource,
			owner: this.node.identity,
			epoch: this.checkpoint.epoch + 1n,
			previous: this.checkpointCid,
			freezes: collection,
			frontier: tips.map((tip) => ({ cid: tip.cid, created: tip.created })),
		});
		await this.node.services.blocks.crashSafeDurability!.barrier();
		await this.writeFloor({
			...this.readFloor()!,
			proposal: snapshot.cid,
		});
		return snapshot.cid;
	}

	private async reconcileFrozen(freezes: readonly string[]): Promise<string[]> {
		fail(
			freezes.length === this.writers.length &&
				new Set(freezes).size === freezes.length,
			"Checkpoint requires one freeze from every writer",
		);
		const collection = [...freezes].sort();
		const manifests = await Promise.all(
			collection.map((cid) =>
				readCheckpointFreeze({
					blocks: this.node.services.blocks,
					cid,
					resource: this.resource,
					writers: this.writers,
					remote: true,
				}),
			),
		);
		const floor = this.readFloor()!;
		fail(
			floor.phase === "frozen" &&
				floor.freeze &&
				collection.includes(floor.freeze),
			"Checkpoint collection omits this writer's durable freeze",
		);
		fail(
			new Set(
				manifests.map((manifest) => toString(manifest.writer.publicKey, "hex")),
			).size === this.writers.length,
			"Checkpoint collection repeats a writer",
		);
		const roots = new Map<string, bigint>();
		for (const manifest of manifests) {
			fail(
				manifest.epoch === this.checkpoint.epoch + 1n &&
					manifest.previous === this.checkpointCid,
				"Freeze belongs to another checkpoint epoch",
			);
			for await (const record of manifest.frontier()) {
				fail(
					!roots.has(record.cid) || roots.get(record.cid) === record.created,
					"Freeze context disagreement",
				);
				roots.set(record.cid, record.created);
			}
			await manifest.retain();
		}
		fail(
			!floor.collection ||
				JSON.stringify(floor.collection) === JSON.stringify(collection),
			"A frozen writer cannot change its checkpoint collection",
		);
		await this.node.services.blocks.crashSafeDurability!.barrier();
		if (!floor.collection) await this.writeFloor({ ...floor, collection });
		const merge = async () => {
			type CausalContext = {
				key: string;
				gid: string;
				timestamp: Timestamp;
			};
			type Frame = {
				cid: string;
				child?: CausalContext;
				context?: CausalContext;
				parents?: string[];
				nextParent: number;
			};
			const assertParent = (fact: Fact, child?: CausalContext) => {
				if (!child) return;
				fail(
					fact.operation.key === child.key && fact.entry.meta.gid === child.gid,
					"Missing or foreign admitted parent",
				);
				fail(
					Timestamp.compare(fact.entry.meta.clock.timestamp, child.timestamp) <
						0,
					"Non-causal checkpoint timestamp",
				);
			};
			for (const cid of roots.keys()) {
				const stack: Frame[] = [{ cid, nextParent: 0 }];
				while (stack.length) {
					const frame = stack[stack.length - 1]!;
					const admitted = this.facts.get(frame.cid);
					if (admitted) {
						assertParent(admitted, frame.child);
						stack.pop();
						continue;
					}
					if (!frame.parents) {
						const raw = await this.node.services.blocks.get(frame.cid, {
							remote: { replicate: false },
						});
						fail(raw, "Missing frozen checkpoint operation");
						const fact = await this.authenticate(raw, frame.cid);
						// Validate before following any reference. Strictly decreasing
						// parent timestamps also reject cycles without recursive calls.
						this.assertEpoch(fact);
						assertParent(fact, frame.child);
						const parents = fact.entry.meta.next;
						fail(
							new Set(parents).size === parents.length,
							"Duplicate checkpoint parents",
						);
						fail(
							(await this.node.services.blocks.put(fact.bytes)) === frame.cid,
							"Frozen checkpoint store returned a different CID",
						);
						// Keep only bounded per-entry metadata on the DFS stack, not
						// every missing entry's raw bytes and decoded document.
						frame.parents = [...parents];
						frame.context = {
							key: fact.operation.key,
							gid: fact.entry.meta.gid,
							timestamp: fact.entry.meta.clock.timestamp,
						};
					}
					if (frame.nextParent < frame.parents.length) {
						stack.push({
							cid: frame.parents[frame.nextParent++]!,
							child: frame.context,
							nextParent: 0,
						});
						continue;
					}
					const raw = await this.node.services.blocks.get(frame.cid, {
						remote: false,
					});
					fail(raw, "Missing retained frozen checkpoint operation");
					const fact = await this.authenticate(raw, frame.cid);
					fact.entry.init(this.shared.log);
					await this.shared.log.join([fact.entry]);
					fail(
						this.facts.has(fact.cid) && (await this.shared.log.has(fact.cid)),
						"Frozen checkpoint operation failed admission",
					);
					// Every successful join crosses the normal durable receive
					// barriers. A retry resumes from this authenticated prefix.
					stack.pop();
				}
			}
			for (const [cid, created] of roots)
				fail(
					this.facts.get(cid)?.created === created,
					"Frozen checkpoint frontier failed validation",
				);
		};
		if ([...roots.keys()].some((cid) => !this.facts.has(cid))) {
			try {
				await this.openInternal({}, merge);
			} finally {
				this.ready = false;
				await this.shared?.close(this);
				await this.projection?.close(this);
			}
		} else {
			await merge();
		}
		return collection;
	}

	/**
	 * Approval requires the exact retained live-key frontier. A writer with an
	 * unreported live branch cannot approve, including after restart.
	 */
	async approveCheckpoint(proposal: string): Promise<CheckpointApproval> {
		return this.transition(() => this.approveCheckpointClosed(proposal));
	}

	private async approveCheckpointClosed(
		proposal: string,
	): Promise<CheckpointApproval> {
		fail(
			this.writers.some((writer) =>
				writer.equals(this.node.identity.publicKey),
			),
			"Only fixed writers approve checkpoints",
		);
		await this.freezeCheckpointClosed();
		fail(
			this.readFloor()?.phase === "frozen",
			"Approval requires a durable frozen writer",
		);
		const pinnedProposal = this.readFloor()?.proposal;
		fail(
			!pinnedProposal || pinnedProposal === proposal,
			"A frozen writer cannot approve a second proposal",
		);
		const snapshot = await readCheckpointSnapshot({
			blocks: this.node.services.blocks,
			cid: proposal,
			resource: this.resource,
			owner: this.owner,
			remote: true,
		});
		fail(
			snapshot.epoch === this.checkpoint.epoch + 1n &&
				snapshot.previous === this.checkpointCid,
			"Invalid proposed checkpoint successor",
		);
		await this.reconcileFrozen(snapshot.freezes);
		const retained = this.retainedFrontier();
		const expected = new Map(retained.map((tip) => [tip.cid, tip.created]));
		for await (const record of snapshot.frontier()) {
			fail(
				expected.get(record.cid) === record.created,
				"Checkpoint frontier differs from this writer's accepted frontier",
			);
			expected.delete(record.cid);
		}
		fail(expected.size === 0, "Checkpoint omits accepted operations");
		// Keep the approved terminal closure independently of the old epoch log.
		// This writer can later activate the certificate without its old donor.
		for (const tip of retained) await this.node.services.blocks.put(tip.bytes);
		await snapshot.retain();
		await this.node.services.blocks.crashSafeDurability!.barrier();
		await this.writeFloor({
			...this.readFloor()!,
			proposal,
		});
		return signCheckpointApproval({
			resource: this.resource,
			proposal,
			identity: this.node.identity,
		});
	}

	/** Publish only after every fixed writer froze and approved the same frontier. */
	async publishCheckpoint(
		proposal: string,
		approvals: CheckpointApproval[],
	): Promise<string> {
		return this.transition(() =>
			this.publishCheckpointClosed(proposal, approvals),
		);
	}

	private async publishCheckpointClosed(
		proposal: string,
		approvals: CheckpointApproval[],
	): Promise<string> {
		fail(
			this.closed && this.node.identity.publicKey.equals(this.owner),
			"Checkpoint publication requires a closed owner",
		);
		const floor = this.readFloor();
		fail(
			floor?.phase === "frozen" && floor.proposal === proposal,
			"Checkpoint proposal is not the frozen owner frontier",
		);
		const certificate = await createCheckpointCertificate({
			blocks: this.node.services.blocks,
			resource: this.resource,
			proposal,
			writers: this.writers.map((writer) => writer.publicKey),
			approvals,
		});
		await this.node.services.blocks.crashSafeDurability!.barrier();
		return certificate.cid;
	}

	protected terminalChildOrder(child: Program): number {
		return child === this.projection ? 1 : 0;
	}

	getTopics(): string[] {
		return this.shared?.rpc.getTopics() ?? [];
	}

	async close(from?: Program): Promise<boolean> {
		const parentIndex =
			this.parents?.findIndex((parent) => parent === from) ?? -1;
		if (!this.closed) {
			if (from && parentIndex === -1) return super.close(from);
			if (parentIndex >= 0 && this.parents.length > 1) return super.close(from);
		}
		this.preventParentAttachments();
		this.ready = false;
		await this.pending;
		return super.close(from);
	}

	async drop(): Promise<boolean> {
		throw new TerminalOperationNotStartedError(
			"Checkpoint history retirement is not supported",
		);
	}
}
