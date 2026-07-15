import { type Constructor, getSchema, variant } from "@dao-xyz/borsh";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { TypedEventEmitter, type TypedEventTarget } from "@libp2p/interface";
import { type Blocks, calculateRawCid } from "@peerbit/blocks-interface";
import { PublicSignKey } from "@peerbit/crypto";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import type { PeerRefs } from "@peerbit/stream-interface";
import { AbortError, TimeoutError } from "@peerbit/time";
import { type Block } from "multiformats/block";
import { type Address } from "./address.js";
import { type Client } from "./client.js";
import {
	type EventOptions,
	Handler,
	type Manageable,
	type ProgramInitializationOptions,
	TERMINAL_BASE_CHECKPOINT,
	TERMINAL_BASE_COMMIT,
	TERMINAL_BASE_RETRY,
	TERMINAL_OUTER_CLEANUP_RELEASE,
	TERMINAL_OUTER_CLEANUP_RETAIN,
	type TerminalBaseCommit,
	TerminalOperationNotStartedError,
	addParent,
} from "./handler.js";
import { getValuesWithType } from "./utils.js";

export class ClosedError extends Error {
	constructor(
		message: string = "Not open. Please invoke 'client.open(...)' before calling this method",
	) {
		super(message);
	}
}

export class MissingAddressError extends Error {
	constructor() {
		super(
			"Address does not exist, please open or save this program once to obtain it",
		);
	}
}

export type OpenProgram = (program: Program) => Promise<Program>;

export interface NetworkEvents {
	join: CustomEvent<PublicSignKey>;
	leave: CustomEvent<PublicSignKey>;
}

export interface LifeCycleEvents {
	drop: CustomEvent<Program>;
	open: CustomEvent<Program>;
	close: CustomEvent<Program>;
}

export interface ProgramEvents extends NetworkEvents, LifeCycleEvents {}

const getAllParentAddresses = (p: Program): string[] => {
	return getAllParent(p, [])
		.filter((x) => x instanceof Program)
		.map((x) => (x as Program).address);
};

const getAllParent = (a: Program, arr: Program[] = [], includeThis = false) => {
	includeThis && arr.push(a);
	if (a.parents) {
		for (const p of a.parents) {
			if (p) {
				getAllParent(p, arr, true);
			}
		}
	}
	return arr;
};

export type ProgramClient = Client<Program>;
class ProgramHandler extends Handler<Program> {
	constructor(properties: { client: ProgramClient }) {
		super({
			identity: properties.client.identity,
			client: properties.client,
			shouldMonitor: (p) => p instanceof Program,
			getDependencies: (program) => program.allPrograms,
			load: Program.load,
		});
	}
}
export { ProgramHandler };

type ExtractArgs<T> = T extends Program<infer Args> ? Args : never;

const PROGRAM_INSTANCE_SYMBOL = Symbol.for("@peerbit/program/Program");

type TerminalOperation = "close" | "drop";
type TerminalCall = {
	type: TerminalOperation;
	from?: Program;
	terminal: boolean;
	promise: Promise<boolean>;
};
type FailedTerminalChild = {
	program: Program;
	operation: TerminalOperation;
	commit?: TerminalBaseCommit;
	cleanupLease?: object;
};
type TerminalRetryContext = {
	commit: TerminalBaseCommit;
	consumed: boolean;
	promise: Promise<boolean>;
};
type PendingTerminalTail = {
	type: TerminalOperation;
	from?: Program;
	hadParentReference: boolean;
	eventEmitted: boolean;
	callbackCompleted: boolean;
};

@variant(0)
export abstract class Program<
	Args = any,
	Events extends ProgramEvents = ProgramEvents,
> implements Manageable<Args>
{
	static [Symbol.hasInstance](instance: any) {
		if (!instance || typeof instance !== "object") {
			return false;
		}

		// Fast path: instances from any @peerbit/program copy that sets the marker.
		if ((instance as any)[PROGRAM_INSTANCE_SYMBOL]) {
			return true;
		}

		// Fallback: allow cross-package "instanceof Program" when multiple copies of
		// @peerbit/program end up in node_modules (e.g. after runtime installs).
		try {
			const schema = getSchema(instance.constructor);
			if (!schema || typeof schema.variant !== "string") {
				return false;
			}
		} catch (_e) {
			return false;
		}

		return (
			typeof (instance as any).beforeOpen === "function" &&
			typeof (instance as any).open === "function" &&
			typeof (instance as any).afterOpen === "function" &&
			typeof (instance as any).close === "function" &&
			typeof (instance as any).drop === "function" &&
			typeof (instance as any).save === "function" &&
			typeof (instance as any).delete === "function" &&
			typeof (instance as any).emitEvent === "function"
		);
	}

	constructor() {
		(this as any)[PROGRAM_INSTANCE_SYMBOL] = true;
	}

	private _node: ProgramClient;
	private _allPrograms: Program[] | undefined;
	private _acceptsParentAttachments = true;

	private _events: TypedEventTarget<ProgramEvents>;
	private _closed: boolean;
	private _terminalCalls: TerminalCall[] | undefined;
	private _dropDeletePending: boolean | undefined;
	private _failedTerminalChildren: FailedTerminalChild[] | undefined;
	private _emittedTerminalEvents: Set<TerminalOperation> | undefined;
	private _terminalBaseCommitVersion: number | undefined;
	private _terminalBaseCommitEpoch: number | undefined;
	private _terminalBaseCommitsByParent:
		| WeakMap<
				Manageable<any>,
				Partial<Record<TerminalOperation, TerminalBaseCommit>>
		  >
		| undefined;
	private _rootTerminalBaseCommits:
		| Partial<Record<TerminalOperation, TerminalBaseCommit>>
		| undefined;
	private _terminalRetryContexts: TerminalRetryContext[] | undefined;
	private _pendingTerminalTail: PendingTerminalTail | undefined;
	private _terminalLifecycleCallbackRunning = false;
	private _terminalOuterCleanupLeases: Set<object> | undefined;
	private _pendingInverseParentReleases: Map<Program, number> | undefined;

	parents: (Program<any> | undefined)[];
	children: Program<Args>[];

	private _address?: Address;

	get address(): Address {
		if (!this._address) {
			throw new MissingAddressError();
		}
		return this._address;
	}

	set address(address: Address) {
		this._address = address;
	}

	get isRoot() {
		return this.parents == null || this.parents.filter((x) => !!x).length === 0;
	}

	get rootAddress(): Address {
		let root: Program = this;
		while (root.parents && root.parents.length > 0) {
			if (root.parents.length > 1) {
				throw new Error("Multiple parents not supported");
			}
			if (root.isRoot) {
				return root.address;
			}
			root = root.parents[0] as Program;
		}
		return root.address;
	}

	async calculateAddress(options?: {
		reset?: boolean;
	}): Promise<{ address: string; block?: Block<any, any, any, any> }> {
		if (this._address && !options?.reset) {
			return { address: this._address, block: undefined };
		}
		const out = await calculateRawCid(serialize(this));
		this._address = out.cid;
		return {
			address: out.cid,
			block: out.block,
		};
	}

	get events(): TypedEventTarget<Events> {
		return this._events || (this._events = new TypedEventEmitter<Events>());
	}

	get closed(): boolean {
		if (this._closed == null) {
			return true;
		}
		return this._closed;
	}
	set closed(closed: boolean) {
		this._closed = closed;
	}

	get acceptsParentAttachments(): boolean {
		// Program clones are borsh-created without running class field initializers.
		return (
			this._acceptsParentAttachments !== false &&
			(this._terminalOuterCleanupLeases?.size ?? 0) === 0
		);
	}

	get pendingTerminalOperation(): TerminalOperation | undefined {
		return (
			this._pendingTerminalTail?.type ||
			(this._dropDeletePending ? "drop" : undefined)
		);
	}

	get terminalLifecycleCallbackRunning(): boolean {
		return this._terminalLifecycleCallbackRunning;
	}

	/**
	 * Fence Handler reuse while a subclass performs irreversible terminal work
	 * before delegating to Program.end(). A subsequent successful reopen clears
	 * the fence in beforeOpen().
	 */
	protected preventParentAttachments(): void {
		this._acceptsParentAttachments = false;
	}

	get node(): ProgramClient {
		return this._node;
	}

	set node(node: ProgramClient) {
		this._node = node;
	}

	private _eventOptions: EventOptions | undefined;

	async beforeOpen(
		node: ProgramClient,
		options?: ProgramInitializationOptions<Args, this>,
	) {
		if ((this._terminalOuterCleanupLeases?.size ?? 0) > 0) {
			throw new Error(
				"Program terminal cleanup must finish before the program can reopen",
			);
		}
		if (this.closed) {
			if (this._pendingTerminalTail || this._dropDeletePending) {
				throw new Error(
					"Program terminal cleanup must finish before the program can reopen",
				);
			}
			this._terminalBaseCommitEpoch = (this._terminalBaseCommitEpoch ?? 0) + 1;
			this._acceptsParentAttachments = true;
			this._failedTerminalChildren = undefined;
			this._emittedTerminalEvents?.clear();
			this._emittedTerminalEvents = undefined;
			this._terminalBaseCommitsByParent = undefined;
			this._rootTerminalBaseCommits = undefined;
		}
		// check that a  discriminator exist
		const schema = getSchema(this.constructor);
		if (!schema || typeof schema.variant !== "string") {
			throw new Error(
				`Expecting class to be decorated with a string variant. Example:\n'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass ${this.constructor.name} { ...`,
			);
		}

		// only store the root program, or programs that have been opened with a parent refernece ("loose programs")
		// TODO do we need addresses for subprograms? if so we also need to call this
		await this.calculateAddress();
		// await this.save(node.services.blocks, { skipOnAddress: true, save: () => !options?.parent || this.isRoot });

		// TODO is this check needed?
		if (getAllParentAddresses(this as Program).includes(this.address)) {
			throw new Error(
				"Subprogram has same address as some parent program. This is not currently supported",
			);
		}

		if (!this.closed) {
			addParent(this, options?.parent);
			return;
		} else {
			addParent(this, options?.parent);
		}

		this._eventOptions = options;
		this.node = node;
		const nexts = [...new Set(this.programs)];
		await Promise.all(
			nexts.map((next) => next.beforeOpen(node, { ...options, parent: this })),
		);

		await this._eventOptions?.onBeforeOpen?.(this);
		this.closed = false;
	}

	async afterOpen() {
		if (!this.closed) {
			await this.node.services.pubsub.addEventListener(
				"subscribe",
				this._subscriptionEventListener ||
					(this._subscriptionEventListener = (s) =>
						!this.closed && this._emitJoinNetworkEvents(s.detail)),
			);
			await this.node.services.pubsub.addEventListener(
				"unsubscribe",
				this._unsubscriptionEventListener ||
					(this._unsubscriptionEventListener = (s) =>
						!this.closed && this._emitLeaveNetworkEvents(s.detail)),
			);

			this.emitEvent(new CustomEvent("open", { detail: this }), true);
			await this._eventOptions?.onOpen?.(this);
			const nexts = [...new Set(this.programs)];
			await Promise.all(nexts.map((next) => next.afterOpen()));
		}
	}

	abstract open(args?: Args): Promise<void>;

	private _clear() {
		this._allPrograms = undefined;
	}
	private _emittedEventsFor: Set<string> | undefined;
	private _peerTopicsByHash:
		| Map<string, { publicKey: PublicSignKey; topics: Set<string> }>
		| undefined;
	private _seedPeerTopicsFromSubscribers?: Promise<void>;
	private get emittedEventsFor(): Set<string> {
		return (this._emittedEventsFor = this._emittedEventsFor || new Set());
	}
	private get peerTopicsByHash(): Map<
		string,
		{ publicKey: PublicSignKey; topics: Set<string> }
	> {
		return (this._peerTopicsByHash =
			this._peerTopicsByHash ||
			new Map<string, { publicKey: PublicSignKey; topics: Set<string> }>());
	}

	private recordPeerSubscription(from: PublicSignKey, topics: string[]) {
		if (!topics || topics.length === 0) return;
		const fromHash = from.hashcode();
		const existing = this.peerTopicsByHash.get(fromHash);
		if (!existing) {
			const set = new Set<string>();
			for (const topic of topics) set.add(topic);
			this.peerTopicsByHash.set(fromHash, { publicKey: from, topics: set });
			return;
		}
		for (const topic of topics) existing.topics.add(topic);
	}

	private recordPeerUnsubscription(from: PublicSignKey, topics: string[]) {
		if (!topics || topics.length === 0) return;
		const fromHash = from.hashcode();
		const existing = this.peerTopicsByHash.get(fromHash);
		if (!existing) return;
		for (const topic of topics) existing.topics.delete(topic);
		if (existing.topics.size === 0) this.peerTopicsByHash.delete(fromHash);
	}

	private peerHasAllTopics(
		entry: { topics: Set<string> },
		allTopics: string[],
	): boolean {
		for (const topic of allTopics) {
			if (!entry.topics.has(topic)) return false;
		}
		return true;
	}

	private getAllTopicsIncludingThis(): string[] {
		const allTopics = [this, ...this.allPrograms]
			// TODO test this code path closed true/false
			.map((x) => x.closed === false && x.getTopics?.())
			.filter((x) => x)
			.flat() as string[];
		return allTopics;
	}

	private async seedPeerTopicsSnapshot(allTopics: string[]) {
		// Subscription events are edge-triggered: if a peer subscribed before this program
		// attached its listeners, we'd miss it and `waitFor()` could hang. Seed an initial
		// snapshot from `pubsub.getSubscribers()` once, then keep it up to date via events.
		//
		// This is best-effort and does not imply the system has global membership knowledge
		// (implementations may only return known peers).
		if (this._seedPeerTopicsFromSubscribers) {
			return this._seedPeerTopicsFromSubscribers;
		}
		this._seedPeerTopicsFromSubscribers = (async () => {
			if (!this.node) return;
			const pubsub = this.node.services.pubsub;
			for (const topic of allTopics) {
				const subscribers = await pubsub.getSubscribers(topic);
				if (!subscribers || subscribers.length === 0) continue;
				for (const subscriber of subscribers) {
					this.recordPeerSubscription(subscriber, [topic]);
					this.emitJoinIfReady(subscriber);
				}
			}
		})();
		return this._seedPeerTopicsFromSubscribers;
	}
	private emitJoinIfReady(from: PublicSignKey) {
		const allTopics = this.getAllTopicsIncludingThis();
		if (allTopics.length === 0) {
			return;
		}

		const fromHash = from.hashcode();
		if (this.emittedEventsFor.has(fromHash)) {
			return;
		}

		const entry = this.peerTopicsByHash.get(fromHash);
		if (!entry) return;
		if (!this.peerHasAllTopics(entry, allTopics)) return;

		this.emittedEventsFor.add(fromHash);
		this.events.dispatchEvent(new CustomEvent("join", { detail: from }));
	}
	private _emitJoinNetworkEvents(s: SubscriptionEvent) {
		this.recordPeerSubscription(s.from, s.topics);
		this.emitJoinIfReady(s.from);
	}

	private _emitLeaveNetworkEvents(s: UnsubcriptionEvent) {
		const allTopics = this.getAllTopicsIncludingThis();
		if (allTopics.length === 0) {
			return; // this is important (see events.spec.ts)
		}

		this.recordPeerUnsubscription(s.from, s.topics);

		const fromHash = s.from.hashcode();
		if (!this.emittedEventsFor.has(fromHash)) return;

		const entry = this.peerTopicsByHash.get(fromHash);
		const hasAllTopics = entry
			? this.peerHasAllTopics(entry, allTopics)
			: false;

		if (hasAllTopics) {
			return; // still here!?
		}

		this.emittedEventsFor.delete(fromHash);
		this.events.dispatchEvent(new CustomEvent("leave", { detail: s.from }));
	}

	private _subscriptionEventListener: (
		e: CustomEvent<SubscriptionEvent>,
	) => void;
	private _unsubscriptionEventListener: (
		e: CustomEvent<UnsubcriptionEvent>,
	) => void;

	[TERMINAL_BASE_CHECKPOINT](): number {
		return this._terminalBaseCommitVersion ?? 0;
	}

	[TERMINAL_OUTER_CLEANUP_RETAIN](): object {
		const lease = {};
		const leases =
			this._terminalOuterCleanupLeases ||
			(this._terminalOuterCleanupLeases = new Set());
		leases.add(lease);
		return lease;
	}

	[TERMINAL_OUTER_CLEANUP_RELEASE](lease: object): void {
		this._terminalOuterCleanupLeases?.delete(lease);
		if (this._terminalOuterCleanupLeases?.size === 0) {
			this._terminalOuterCleanupLeases = undefined;
			this.reconcilePendingInverseParentReleases();
		}
	}

	[TERMINAL_BASE_COMMIT](
		afterVersion: number,
		type: TerminalOperation,
		from?: Manageable<any>,
	): TerminalBaseCommit | undefined {
		const commit = from
			? this._terminalBaseCommitsByParent?.get(from)?.[type]
			: this._rootTerminalBaseCommits?.[type];
		return commit && commit.version > afterVersion ? commit : undefined;
	}

	async [TERMINAL_BASE_RETRY](
		commit: TerminalBaseCommit,
		operation: () => Promise<boolean>,
	): Promise<boolean> {
		if (commit.epoch !== (this._terminalBaseCommitEpoch ?? 0)) {
			throw new Error(
				"Program terminal cleanup belongs to a stale open lifecycle",
			);
		}
		const context: TerminalRetryContext = {
			commit,
			consumed: false,
			promise: Promise.resolve(commit.result),
		};
		const contexts =
			this._terminalRetryContexts || (this._terminalRetryContexts = []);
		contexts.push(context);
		try {
			return await operation();
		} finally {
			const index = contexts.indexOf(context);
			if (index !== -1) contexts.splice(index, 1);
			if (contexts.length === 0) this._terminalRetryContexts = undefined;
		}
	}

	private consumeTerminalRetry(
		type: TerminalOperation,
		from?: Program,
	): Promise<boolean> | undefined {
		const context = this._terminalRetryContexts?.find(
			(candidate) =>
				!candidate.consumed &&
				candidate.commit.type === type &&
				candidate.commit.from === from,
		);
		if (!context) return undefined;
		context.consumed = true;
		return context.promise;
	}

	private recordTerminalBaseCommit(
		type: TerminalOperation,
		from: Program | undefined,
		result: boolean,
		releasedParentReferences: number,
	): void {
		const version = (this._terminalBaseCommitVersion ?? 0) + 1;
		this._terminalBaseCommitVersion = version;
		const commit = {
			epoch: this._terminalBaseCommitEpoch ?? 0,
			version,
			type,
			from,
			result,
			releasedParentReferences,
		};
		if (from) {
			const commits =
				this._terminalBaseCommitsByParent?.get(from) ??
				({} as Partial<Record<TerminalOperation, TerminalBaseCommit>>);
			commits[type] = commit;
			const commitsByParent =
				this._terminalBaseCommitsByParent ||
				(this._terminalBaseCommitsByParent = new WeakMap());
			commitsByParent.set(from, commits);
		} else {
			const commits =
				this._rootTerminalBaseCommits || (this._rootTerminalBaseCommits = {});
			commits[type] = commit;
		}
	}

	private retainInverseParentRelease(from: Program | undefined): void {
		if (!from) return;
		const pending =
			this._pendingInverseParentReleases ||
			(this._pendingInverseParentReleases = new Map());
		pending.set(from, (pending.get(from) ?? 0) + 1);
	}

	private reconcilePendingInverseParentReleases(from?: Program): void {
		const pending = this._pendingInverseParentReleases;
		if (!pending) return;
		const parents = from ? [from] : [...pending.keys()];
		for (const parent of parents) {
			let releasedReferences = pending.get(parent) ?? 0;
			const retainedReferences =
				this.parents?.filter((candidate) => candidate === parent).length ?? 0;
			while (releasedReferences > 0) {
				const inverseReferences =
					parent.children?.filter((candidate) => candidate === this).length ??
					0;
				if (inverseReferences > retainedReferences) {
					const childIndex = parent.children.indexOf(this);
					if (childIndex !== -1) parent.children.splice(childIndex, 1);
				}
				releasedReferences -= 1;
			}
			pending.delete(parent);
		}
		if (pending.size === 0) this._pendingInverseParentReleases = undefined;
	}

	private isOutermostBaseTerminalOperation(type: TerminalOperation): boolean {
		const current = type === "close" ? this.close : this.drop;
		const base =
			type === "close" ? Program.prototype.close : Program.prototype.drop;
		return current === base;
	}

	private async processEnd(
		type: TerminalOperation,
		from: Program | undefined,
		hadParentReference: boolean,
	) {
		if (this.closed) {
			this._clear();
			return true;
		}
		// Lifecycle events retain their historical attempt semantics and parent-first
		// ordering. A failed attempt emits once; retries in the same open epoch do not
		// emit a duplicate event. The awaited onClose/onDrop callback below remains a
		// committed-tail callback and only runs after children have drained.
		const emittedTerminalEvents =
			this._emittedTerminalEvents ||
			(this._emittedTerminalEvents = new Set<TerminalOperation>());
		const retryingTerminalAttempt = emittedTerminalEvents.has(type);
		if (!emittedTerminalEvents.has(type)) {
			emittedTerminalEvents.add(type);
			this.emitEvent(new CustomEvent(type, { detail: this }), true);
		}

		const children = [...(this.children ?? [])];
		const previousFailures = [...(this._failedTerminalChildren ?? [])];
		const claimedFailures = new Set<FailedTerminalChild>();
		const attempts: {
			child: Program;
			operation: TerminalOperation;
			previous?: FailedTerminalChild;
			checkpoint: number;
			startedClosed: boolean;
			ownerReferencesBefore: number;
			skipped: boolean;
			cleanupLease?: object;
		}[] = [];
		const tailsByChild = new Map<Program, Promise<boolean>>();
		const childPromises = children.map((child) => {
			const previous = previousFailures.find(
				(failure) => failure.program === child && !claimedFailures.has(failure),
			);
			if (previous) claimedFailures.add(previous);
			const attempt = {
				child,
				operation:
					previous?.operation ?? child.pendingTerminalOperation ?? type,
				previous,
				checkpoint: 0,
				startedClosed: false,
				ownerReferencesBefore: 0,
				skipped: false,
				cleanupLease: previous?.cleanupLease,
			};
			attempts.push(attempt);
			const run = async () => {
				attempt.startedClosed = child.closed;
				attempt.ownerReferencesBefore =
					child.parents?.filter((parent) => parent === this).length ?? 0;
				if (
					child.closed &&
					(attempt.operation === "close" || retryingTerminalAttempt) &&
					!previous &&
					!child.pendingTerminalOperation
				) {
					return true;
				}
				attempt.cleanupLease ??= child[TERMINAL_OUTER_CLEANUP_RETAIN]();
				attempt.checkpoint = child[TERMINAL_BASE_CHECKPOINT]();
				const invoke = () => child[attempt.operation](this as Program);
				const result = await (previous?.commit
					? child[TERMINAL_BASE_RETRY](previous.commit, invoke)
					: invoke());
				const ownerReferencesAfter =
					child.parents?.filter((parent) => parent === this).length ?? 0;
				const committedRelease =
					(previous?.commit?.releasedParentReferences ?? 0) > 0;
				if (
					!attempt.startedClosed &&
					!child.closed &&
					!committedRelease &&
					ownerReferencesAfter >= attempt.ownerReferencesBefore
				) {
					throw new Error(
						`Child program at ${child.address} did not release parent ownership during ${attempt.operation}`,
					);
				}
				return result;
			};
			const predecessor = tailsByChild.get(child);
			const promise = predecessor
				? predecessor.then(run, (error) => {
						// Duplicate appearances represent distinct ownership references.
						// Do not spend the next reference while cleanup for the preceding
						// release is unresolved; retain it for the recovery attempt.
						attempt.skipped = true;
						throw error;
					})
				: run();
			tailsByChild.set(child, promise);
			return promise;
		});
		const results = await Promise.allSettled(childPromises);
		const failedChildren: FailedTerminalChild[] = [];
		let firstChildError: unknown;
		for (let index = 0; index < results.length; index++) {
			const result = results[index]!;
			const attempt = attempts[index]!;
			const child = attempt.child;
			if (result.status === "rejected") {
				if (attempt.skipped) {
					if (attempt.previous) failedChildren.push(attempt.previous);
					firstChildError ??= result.reason;
					continue;
				}
				if (!attempt.startedClosed || attempt.previous) {
					const ownerReferencesAfter =
						child.parents?.filter((parent) => parent === this).length ?? 0;
					const commit =
						attempt.previous?.commit ||
						child[TERMINAL_BASE_COMMIT](
							attempt.checkpoint,
							attempt.operation,
							this,
						);
					const baseProgressed =
						commit != null ||
						child.closed !== attempt.startedClosed ||
						ownerReferencesAfter !== attempt.ownerReferencesBefore;
					const retainCleanupLease =
						baseProgressed ||
						attempt.previous != null ||
						!(result.reason instanceof TerminalOperationNotStartedError);
					const cleanupLease = retainCleanupLease
						? attempt.cleanupLease
						: undefined;
					if (!retainCleanupLease && attempt.cleanupLease) {
						child[TERMINAL_OUTER_CLEANUP_RELEASE](attempt.cleanupLease);
						attempt.cleanupLease = undefined;
					}
					failedChildren.push({
						program: child,
						operation:
							attempt.operation === "drop" && !baseProgressed
								? "drop"
								: commit || child.closed
									? attempt.operation
									: "close",
						commit,
						cleanupLease,
					});
				} else if (attempt.cleanupLease) {
					child[TERMINAL_OUTER_CLEANUP_RELEASE](attempt.cleanupLease);
					attempt.cleanupLease = undefined;
				}
				firstChildError ??= result.reason;
				continue;
			}
			if (attempt.cleanupLease) {
				child[TERMINAL_OUTER_CLEANUP_RELEASE](attempt.cleanupLease);
				attempt.cleanupLease = undefined;
			}
			// A managed child's Handler may already have reconciled the inverse edge
			// from its committed base release. Nested unmanaged children rely on this
			// parent instead. Remove at most the one excess edge represented by this
			// successful occurrence so both paths remain count-correct.
			const childReferences =
				child.parents?.filter((parent) => parent === this).length ?? 0;
			const childEdges =
				this.children?.filter((candidate) => candidate === child).length ?? 0;
			if (childEdges > childReferences) {
				const childIndex = this.children.indexOf(child);
				if (childIndex !== -1) this.children.splice(childIndex, 1);
			}
		}
		for (const previous of previousFailures) {
			if (!claimedFailures.has(previous) && previous.cleanupLease) {
				previous.program[TERMINAL_OUTER_CLEANUP_RELEASE](previous.cleanupLease);
				previous.cleanupLease = undefined;
			}
		}
		this._failedTerminalChildren =
			failedChildren.length > 0 ? failedChildren : undefined;
		if (firstChildError !== undefined) throw firstChildError;

		this._clear();
		this.closed = true;
		this._pendingTerminalTail = {
			type,
			from,
			hadParentReference,
			eventEmitted: true,
			callbackCompleted: false,
		};
		await this.finishTerminalTail();
		return true;
	}

	private async finishTerminalTail(): Promise<void> {
		const tail = this._pendingTerminalTail;
		if (!tail) return;
		if (!tail.eventEmitted) {
			tail.eventEmitted = true;
			const emittedTerminalEvents =
				this._emittedTerminalEvents ||
				(this._emittedTerminalEvents = new Set<TerminalOperation>());
			if (!emittedTerminalEvents.has(tail.type)) {
				emittedTerminalEvents.add(tail.type);
				this.emitEvent(new CustomEvent(tail.type, { detail: this }), true);
			}
		}
		if (!tail.callbackCompleted) {
			const callback =
				tail.type === "close"
					? this._eventOptions?.onClose
					: this._eventOptions?.onDrop;
			this._terminalLifecycleCallbackRunning = true;
			try {
				await callback?.(this);
			} finally {
				this._terminalLifecycleCallbackRunning = false;
			}
			tail.callbackCompleted = true;
		}

		this.node?.services.pubsub.removeEventListener(
			"subscribe",
			this._subscriptionEventListener,
		);
		this.node?.services.pubsub.removeEventListener(
			"unsubscribe",
			this._unsubscriptionEventListener,
		);
		this._emittedEventsFor?.clear();
		this._emittedEventsFor = undefined;
		this._peerTopicsByHash?.clear();
		this._peerTopicsByHash = undefined;
		this._seedPeerTopicsFromSubscribers = undefined;
		this._eventOptions = undefined;
		if (tail.hadParentReference) {
			const parentIndex =
				this.parents?.findIndex((parent) => parent === tail.from) ?? -1;
			if (parentIndex !== -1) {
				this.parents.splice(parentIndex, 1);
				this.retainInverseParentRelease(tail.from);
			}
		}
		this._pendingTerminalTail = undefined;
	}

	private async end(type: TerminalOperation, from?: Program): Promise<boolean> {
		if (this.closed) {
			if (
				this._pendingTerminalTail &&
				this._pendingTerminalTail.type !== type
			) {
				throw new Error(
					`Program has pending ${this._pendingTerminalTail.type} cleanup; retry it before ${type}`,
				);
			}
			if (this._pendingTerminalTail) {
				await this.finishTerminalTail();
				return true;
			}
			if (this._dropDeletePending && type === "close") {
				throw new Error(
					"Program has pending drop deletion; retry drop before close",
				);
			}
			if (type === "drop") {
				throw new ClosedError("Program is closed, can not drop");
			}
			return true;
		}

		let parentIdx = -1;
		let close = true;
		if (this.parents) {
			parentIdx = this.parents.findIndex((x) => x === from);
			if (parentIdx !== -1) {
				if (this.parents.length === 1) {
					close = true;
				} else {
					this.parents.splice(parentIdx, 1);
					this.retainInverseParentRelease(from);
					close = false;
				}
			} else if (from) {
				throw new TerminalOperationNotStartedError(
					"Could not find from in parents",
				);
			}
		}

		if (close) {
			// Establish the reuse fence synchronously once this call owns terminal
			// shutdown. Non-terminal parent releases remain attachable.
			this.preventParentAttachments();
		}
		return close && (await this.processEnd(type, from, parentIdx !== -1));
	}
	private performTerminalOperation(
		type: TerminalOperation,
		from?: Program,
	): Promise<boolean> {
		if (type === "close") {
			this._emittedEventsFor = undefined;
			return this.end("close", from);
		}
		return this.performDrop(from);
	}

	private async performDrop(from?: Program): Promise<boolean> {
		if (this.closed && this._dropDeletePending) {
			await this.delete();
			this._dropDeletePending = false;
			return true;
		}

		const dropped = await this.end("drop", from);
		if (dropped) {
			// Remember the committed terminal transition before deletion. If rm()
			// fails, a later drop retry must resume here instead of rejecting merely
			// because Program.end() already made `closed` observable.
			this._dropDeletePending = true;
			await this.delete();
			this._dropDeletePending = false;
		}
		return dropped;
	}

	private terminalOperation(
		type: TerminalOperation,
		from?: Program,
	): Promise<boolean> {
		if (this._terminalLifecycleCallbackRunning) {
			return Promise.reject(
				new TerminalOperationNotStartedError(
					"Program lifecycle callbacks cannot wait for their own terminal operation",
				),
			);
		}
		const retry = this.consumeTerminalRetry(type, from);
		if (retry) return retry;
		const calls = this._terminalCalls || (this._terminalCalls = []);
		const matching = calls.find(
			(call) => call.type === type && call.from === from && call.terminal,
		);
		if (matching) {
			return matching.promise;
		}

		if (type === "close") {
			// Drop is the stronger terminal operation. A close admitted for the same
			// owner while drop is pending observes that exact operation and promise.
			const dropping = calls.find(
				(call) => call.type === "drop" && call.from === from && call.terminal,
			);
			if (dropping) {
				return dropping.promise;
			}
		}

		const predecessor = calls.at(-1);
		let resolve!: (value: boolean) => void;
		let reject!: (reason?: unknown) => void;
		const promise = new Promise<boolean>((promiseResolve, promiseReject) => {
			resolve = promiseResolve;
			reject = promiseReject;
		});
		const parentIndex =
			this.parents?.findIndex((parent) => parent === from) ?? -1;
		const terminal =
			this.closed || parentIndex === -1 || (this.parents?.length ?? 0) === 1;
		const call: TerminalCall = { type, from, terminal, promise };
		calls.push(call);

		const execute = () => {
			let operation: Promise<boolean>;
			const ownerReferencesBefore =
				this.parents?.filter((parent) => parent === from).length ?? 0;
			try {
				operation = this.performTerminalOperation(type, from);
			} catch (error) {
				reject(error);
				return;
			}
			void operation.then((result) => {
				const ownerReferencesAfter =
					this.parents?.filter((parent) => parent === from).length ?? 0;
				this.recordTerminalBaseCommit(
					type,
					from,
					result,
					Math.max(0, ownerReferencesBefore - ownerReferencesAfter),
				);
				// Program only owns the base promise. A subclass may still be doing
				// post-super terminal cleanup, so managed/embedded calls defer inverse-
				// edge removal to the caller's outer cleanup lease. A non-terminal owner
				// release leaves the child live and can reconcile immediately; an
				// inherited base method is itself the full public operation.
				if (
					(this._terminalOuterCleanupLeases?.size ?? 0) === 0 &&
					(!result || this.isOutermostBaseTerminalOperation(type))
				) {
					this.reconcilePendingInverseParentReleases(result ? undefined : from);
				}
				resolve(result);
			}, reject);
		};
		if (predecessor) {
			// Different owners and close-vs-drop calls are serialized. In particular,
			// a drop admitted after close does not pretend destructive cleanup happened:
			// it runs after close and receives ClosedError from the now-closed program.
			void predecessor.promise.then(execute, execute);
		} else {
			execute();
		}

		void promise.then(
			() => this.finishTerminalCall(call),
			() => this.finishTerminalCall(call),
		);
		return promise;
	}

	private finishTerminalCall(call: TerminalCall): void {
		const calls = this._terminalCalls;
		const index = calls?.indexOf(call) ?? -1;
		if (index !== -1) {
			calls!.splice(index, 1);
		}
		if (calls?.length === 0) {
			this._terminalCalls = undefined;
		}
	}

	close(from?: Program): Promise<boolean> {
		this._emittedEventsFor = undefined;
		return this.terminalOperation("close", from);
	}

	drop(from?: Program): Promise<boolean> {
		return this.terminalOperation("drop", from);
	}

	emitEvent(event: CustomEvent, parents = false) {
		this.events.dispatchEvent(event);
		if (parents) {
			if (this.parents) {
				for (const parent of this.parents) {
					parent?.emitEvent(event, parents);
				}
			}
		}
	}

	/**
	 * Wait for another peer to be 'ready' to talk with you for this particular program
	 * @param other
	 * @throws TimeoutError if the timeout is reached
	 */
	async waitFor(
		other: PeerRefs,
		options?: {
			seek?: "any" | "present";
			signal?: AbortSignal;
			timeout?: number;
		},
	): Promise<string[]> {
		// make sure nodes are reachable
		let expectedHashes = await this.node.services.pubsub.waitFor(other, {
			seek: options?.seek,
			signal: options?.signal,
			timeout: options?.timeout,
		});

		const allTopics = this.getAllTopicsIncludingThis();
		if (allTopics.length === 0) {
			throw new Error("Program has no topics, cannot get ready");
		}
		const pubsub = this.node.services.pubsub;
		const collectProvidedPublicKeys = (
			refs: PeerRefs,
		): Map<string, PublicSignKey> => {
			const providedPublicKeysByHash = new Map<string, PublicSignKey>();
			const rememberPublicKey = (ref: unknown) => {
				if (ref instanceof PublicSignKey) {
					providedPublicKeysByHash.set(ref.hashcode(), ref);
				}
			};

			if (refs instanceof PublicSignKey || typeof refs === "string") {
				rememberPublicKey(refs);
				return providedPublicKeysByHash;
			}
			if (refs instanceof Array || refs instanceof Set) {
				for (const ref of refs) {
					rememberPublicKey(ref);
				}
				return providedPublicKeysByHash;
			}
			if (
				typeof (refs as Iterable<unknown>)?.[Symbol.iterator] === "function"
			) {
				for (const ref of refs as Iterable<unknown>) {
					rememberPublicKey(ref);
				}
			}

			return providedPublicKeysByHash;
		};
		const providedKeysByHash = collectProvidedPublicKeys(other);

		// Seed a current subscriber snapshot after reachability is established so callers
		// don't depend solely on edge-triggered subscribe events or follow-up snapshot
		// requests to observe peers that were already ready.
		await this.seedPeerTopicsSnapshot(allTopics);

		// Prefer a direct neighbour stream when available. This avoids cases where
		// peers are "reachable" via the routing table but we haven't established
		// a writable protocol stream yet (initial control-plane gossip can be dropped).
		const neighborProbeTimeout = Math.min(options?.timeout ?? 10_000, 3_000);
		await Promise.all(
			expectedHashes.map((hash) =>
				pubsub
					.waitFor(hash, { target: "neighbor", timeout: neighborProbeTimeout })
					.catch(() => {
						// Multi-hop overlays may never be direct neighbours; best-effort only.
					}),
			),
		);

		// Best-effort seeding: subscribe events are edge-triggered and can be missed if a peer
		// subscribed before this program attached listeners. Actively ask for subscriber
		// snapshots while waiting, but rate-limit to avoid fanout in larger overlays.
		const REQUEST_MIN_INTERVAL_MS = 500;
		const lastRequestAtByPeer = new Map<string, number>();
		const publicKeyPromisesByHash = new Map<
			string,
			Promise<PublicSignKey | undefined>
		>();
		const getPublicKeyForHash = (
			hash: string,
		): Promise<PublicSignKey | undefined> => {
			let existing = publicKeyPromisesByHash.get(hash);
			if (!existing) {
				const providedKey = providedKeysByHash.get(hash);
				existing = providedKey
					? Promise.resolve(providedKey)
					: Promise.resolve(pubsub.getPublicKey(hash)).catch(
							(): PublicSignKey | undefined => undefined,
						);
				publicKeyPromisesByHash.set(hash, existing);
			}
			return existing;
		};
		const requestSubscriberSnapshots = (hash: string): void => {
			const now = Date.now();
			const last = lastRequestAtByPeer.get(hash) ?? 0;
			if (now - last < REQUEST_MIN_INTERVAL_MS) return;
			lastRequestAtByPeer.set(hash, now);

			void getPublicKeyForHash(hash).then((publicKey) => {
				for (const topic of allTopics) {
					void Promise.resolve(
						pubsub.requestSubscribers(topic, publicKey),
					).catch(() => {
						// best-effort; the wait loop will retry and/or time out
					});
				}
			});
		};

		for (const hash of expectedHashes) {
			requestSubscriberSnapshots(hash);
		}

		// wait for subscribing to topics
		return new Promise<string[]>((resolve, reject) => {
			let settled = false;
			// Historically this was ~20s; keep enough headroom for initial control-plane
			// convergence (stream establishment + subscription gossip) in sparse overlays.
			const timeoutMs = options?.timeout || 20 * 1000;
			let timeout: ReturnType<typeof setTimeout> | undefined = undefined;
			let listener: (() => void) | undefined = undefined;
			let poll: ReturnType<typeof setInterval> | undefined = undefined;
			let checking = false;

			const clear = () => {
				if (listener) {
					this.node.services.pubsub.removeEventListener("subscribe", listener);
				}
				options?.signal?.removeEventListener("abort", abortListener);
				this.events.removeEventListener("close", closeListener);
				this.events.removeEventListener("drop", dropListener);
				timeout && clearTimeout(timeout);
				poll && clearInterval(poll);
				timeout = undefined;
				poll = undefined;
			};

			const resolveOnce = (hashes: string[]) => {
				if (settled) return;
				settled = true;
				clear();
				resolve(hashes);
			};

			const rejectOnce = (error: unknown) => {
				if (settled) return;
				settled = true;
				clear();
				reject(error);
			};

			const abortListener = (e: Event) => {
				const reason = (e.target as any)?.reason;
				rejectOnce(
					reason instanceof Error
						? reason
						: options?.signal?.reason instanceof Error
							? options.signal.reason
							: new AbortError("Aborted"),
				);
			};

			const closeListener = () => rejectOnce(new AbortError("Program closed"));
			const dropListener = () => rejectOnce(new AbortError("Program dropped"));

			if (options?.signal?.aborted) {
				rejectOnce(
					options.signal.reason instanceof Error
						? options.signal.reason
						: new AbortError("Aborted"),
				);
				return;
			}

			timeout = setTimeout(() => {
				rejectOnce(new TimeoutError("Timeout waiting for replicating peer"));
			}, timeoutMs);

			options?.signal?.addEventListener("abort", abortListener, { once: true });
			this.events.addEventListener("close", closeListener, { once: true });
			this.events.addEventListener("drop", dropListener, { once: true });

			type PubSubTopicIndex = {
				topics?: Map<string, Map<string, { publicKey: PublicSignKey }>>;
			};
			const pubsubTopicIndex = pubsub as PubSubTopicIndex;

			const isPeerReady = async (hash: string) => {
				const cached = this._peerTopicsByHash?.get(hash);
				if (cached && this.peerHasAllTopics(cached, allTopics)) {
					return true;
				}

				// Subscription events are edge-triggered: we may have missed earlier subscribe
				// events (e.g. if they arrived during program open). Fall back to best-effort
				// pubsub snapshots to avoid false timeouts.
				let key: PublicSignKey | undefined = providedKeysByHash.get(hash);
				const observedTopics: string[] = [];
				const recordObservedTopics = () => {
					if (key && observedTopics.length > 0) {
						this.recordPeerSubscription(key, observedTopics);
						this.emitJoinIfReady(key);
					}
				};

				for (const topic of allTopics) {
					// Fast path for TopicControlPlane: O(1) membership check without allocations.
					const topicPeers = pubsubTopicIndex.topics?.get(topic);
					if (topicPeers?.has?.(hash)) {
						key ||= topicPeers.get(hash)?.publicKey;
						observedTopics.push(topic);
						continue;
					}

					const subscribers =
						await this.node.services.pubsub.getSubscribers(topic);
					if (!subscribers || subscribers.length === 0) {
						recordObservedTopics();
						return false;
					}
					const found = subscribers.find((x) => x.hashcode() === hash);
					if (!found) {
						recordObservedTopics();
						return false;
					}
					key ||= found;
					observedTopics.push(topic);
				}

				recordObservedTopics();
				return observedTopics.length === allTopics.length;
			};

			const checkReady = async () => {
				if (settled) {
					return;
				}
				if (checking) {
					return;
				}
				checking = true;
				let ready = true;
				try {
					for (const hash of expectedHashes) {
						if (!(await isPeerReady(hash))) {
							requestSubscriberSnapshots(hash);
							ready = false;
							break;
						}
					}
					if (ready) {
						resolveOnce(expectedHashes);
					}
				} catch (error) {
					rejectOnce(error);
				} finally {
					checking = false;
				}
			};
			listener = () => {
				void checkReady();
			};
			this.node.services.pubsub.addEventListener("subscribe", listener);
			poll = setInterval(() => void checkReady(), 200);
			poll.unref?.();
			checkReady();
		});
	}

	async getReady(): Promise<Map<string, PublicSignKey>> {
		// Observed peers that subscribe to all topics.
		// We intentionally do not depend on `pubsub.getSubscribers()` since that implies
		// global membership knowledge which does not scale for large overlays.
		const allTopics = this.getAllTopicsIncludingThis();
		if (allTopics.length === 0) {
			throw new Error("Program has no topics, cannot get ready");
		}

		const ready = new Map<string, PublicSignKey>();
		await this.seedPeerTopicsSnapshot(allTopics);

		for (const [hash, entry] of this.peerTopicsByHash) {
			if (!this.peerHasAllTopics(entry, allTopics)) continue;
			ready.set(hash, entry.publicKey);
		}
		return ready;
	}

	get allPrograms(): Program[] {
		if (this._allPrograms) {
			return this._allPrograms;
		}
		const arr: Program[] = [];
		const pending = [...this.programs];
		const visited = new Set<Program>([this]);
		while (pending.length > 0) {
			const next = pending.pop()!;
			if (visited.has(next)) continue;
			visited.add(next);
			arr.push(next);
			const nested = (next as Program & { programs?: Program[] }).programs;
			pending.push(
				...(Array.isArray(nested) ? nested : getValuesWithType(next, Program)),
			);
		}
		this._allPrograms = arr;
		return this._allPrograms;
	}

	get programs(): Program[] {
		return getValuesWithType(this, Program);
	}

	clone(): this {
		return deserialize(serialize(this), this.constructor);
	}

	getTopics?(): string[];

	async save(
		store: Blocks = this.node.services.blocks,
		options?: {
			reset?: boolean;
			skipOnAddress?: boolean;
			save?: (address: string) => boolean | Promise<boolean>;
		},
	): Promise<Address> {
		const existingAddress = this._address;
		if (existingAddress) {
			if (options?.skipOnAddress) {
				return existingAddress;
			}
		}

		// always reset the address on save
		const toPut = await this.calculateAddress({ reset: true }); // this will also set the address (this.address)

		if (
			!options?.reset &&
			existingAddress &&
			existingAddress !== this.address
		) {
			this._address = existingAddress;
			throw new Error(
				"Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized",
			);
		}

		if (
			(options?.save && !(await options.save(toPut.address))) ||
			(await store.has(toPut.address))
		) {
			return this.address!;
		}

		await store.put(
			toPut.block
				? { cid: toPut.address, block: toPut.block }
				: serialize(this),
		);

		if (options?.reset) {
			// delete the old address if it exists and different from the new one
			if (existingAddress && existingAddress !== this.address) {
				await store.rm(existingAddress);
			}
		}

		return this.address!;
	}

	async delete(): Promise<void> {
		if (this._address) {
			return this.node.services.blocks.rm(this.address);
		}
		// Not saved
	}

	static async load<P extends Program<any>>(
		address: Address,
		store: Blocks,
		options?: {
			timeout?: number;
		},
	): Promise<P | undefined> {
		const bytes = await store.get(address, {
			remote: {
				timeout: options?.timeout,
				priority: 1,
				replicate: true,
			},
		});
		if (!bytes) {
			return undefined;
		}
		const der = deserialize(bytes, Program);
		der.address = address;
		return der as P;
	}

	static async open<T extends Program<ExtractArgs<T>>>(
		this: Constructor<T>,
		address: string,
		node: ProgramClient,
		options?: ProgramInitializationOptions<ExtractArgs<T>, T>,
	): Promise<T> {
		const p = await Program.load<T>(address, node.services.blocks);

		if (!p) {
			throw new Error("Failed to load program");
		}
		await node.open<any>(p, options as any); // TODO fix types
		return p as T;
	}
}

export const getProgramFromVariants = <
	T extends Program,
>(): Constructor<T>[] => {
	const deps = (Program.prototype as any)[1000]; /// TODO improve BORSH lib to provide all necessary utility methods
	return (deps || []) as Constructor<T>[];
};

export const getProgramFromVariant = <T extends Program>(
	variant: string,
): Constructor<T> | undefined => {
	return getProgramFromVariants().filter(
		(x) => getSchema(x).variant === variant,
	)[0] as Constructor<T>;
};
