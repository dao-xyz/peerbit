import { type Blocks } from "@peerbit/blocks-interface";
import type { Identity } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import PQueue from "p-queue";
import type { Address } from "./address.js";

export const logger = loggerFn("peerbit:program:handler");

type ProgramMergeStrategy = "replace" | "reject" | "reuse";
type HandlerLifecycleState = "running" | "stopping" | "stopped" | "failed";
export type TerminalOperation = "close" | "drop";
export type TerminalBaseCommit = {
	epoch: number;
	version: number;
	type: TerminalOperation;
	from?: Manageable<any>;
	result: boolean;
	releasedParentReferences: number;
};
export const TERMINAL_BASE_CHECKPOINT = Symbol.for(
	"@peerbit/program/terminal-base-checkpoint",
);
export const TERMINAL_BASE_COMMIT = Symbol.for(
	"@peerbit/program/terminal-base-commit",
);
export const TERMINAL_BASE_RETRY = Symbol.for(
	"@peerbit/program/terminal-base-retry",
);
export const TERMINAL_OUTER_CLEANUP_RETAIN = Symbol.for(
	"@peerbit/program/terminal-outer-cleanup-retain",
);
export const TERMINAL_OUTER_CLEANUP_RELEASE = Symbol.for(
	"@peerbit/program/terminal-outer-cleanup-release",
);
const TERMINAL_OPERATION_NOT_STARTED = Symbol.for(
	"@peerbit/program/terminal-operation-not-started-error",
);

/**
 * A terminal-operation precondition rejected the call before any lifecycle or
 * resource mutation began. Handler leaves the program retryable instead of
 * retaining this as partially completed cleanup.
 */
export class TerminalOperationNotStartedError extends Error {
	readonly [TERMINAL_OPERATION_NOT_STARTED] = true;

	constructor(message: string) {
		super(message);
		this.name = "TerminalOperationNotStartedError";
	}

	static [Symbol.hasInstance](instance: unknown): boolean {
		return Boolean(
			instance &&
				typeof instance === "object" &&
				(instance as Record<symbol, unknown>)[
					TERMINAL_OPERATION_NOT_STARTED
				] === true,
		);
	}
}
export type ExtractArgs<T> = T extends CanOpen<infer Args> ? Args : never;

type FailedTerminalCall = {
	type: TerminalOperation;
	args: any[];
	commit?: TerminalBaseCommit;
	releasedParentReferences?: number;
};

type OuterTerminalCall = {
	type: TerminalOperation;
	from?: Manageable<any>;
	terminal: boolean;
	claimedOwnerReference: boolean;
	invokedWrapper: (...args: any[]) => Promise<boolean>;
	ownerReferencesBeforeInvoke?: number;
	ownerReferencesAfterInvoke?: number;
	promise: Promise<boolean>;
};

type TerminalOperationState<T extends Manageable<any>> = {
	program: T;
	address: Address;
	activeCalls: number;
	pending: Set<Promise<boolean>>;
	failed: boolean;
	failedOperation?: TerminalOperation;
	failedCall?: FailedTerminalCall;
	terminalCompleted: boolean;
	outerTail: Promise<void>;
	outerCalls: Set<OuterTerminalCall>;
	closeWrapper?: (...args: any[]) => Promise<boolean>;
	dropWrapper?: (...args: any[]) => Promise<boolean>;
	closeOperation?: (...args: any[]) => Promise<boolean>;
	dropOperation?: (...args: any[]) => Promise<boolean>;
	synchronouslyInvokingWrapper?: {
		type: TerminalOperation;
		wrapper: (...args: any[]) => Promise<boolean>;
		args: any[];
	};
	cleanupLease?: object;
};

type CleanupResidual = {
	program: Manageable<any>;
	failures: FailedTerminalCall[];
	terminalState?: TerminalOperationState<any>;
	cleanupLease?: object;
	activeReservations: number;
};

type ArrayPropertyBaseline<V> = {
	hadOwnProperty: boolean;
	value: V[] | undefined;
};

type ArrayPropertyState<V> = ArrayPropertyBaseline<V> & {
	reference: V[] | undefined;
};

type InitializationRollbackOwnerPhase =
	| "pending-pre-base"
	| "committed"
	| "consumed";

type InitializationRollbackOwner = {
	address: Address;
	owner: Manageable<any> | undefined;
	phase: InitializationRollbackOwnerPhase;
	parents: ArrayPropertyBaseline<Manageable<any> | undefined>;
	children: ArrayPropertyBaseline<Manageable<any>>;
	ownerChildren?: ArrayPropertyBaseline<Manageable<any>>;
	ownerChildrenWorkingReference?: Manageable<any>[];
	ownerChildrenWorkingSnapshot?: ArrayPropertyBaseline<Manageable<any>>;
	ownerChildrenDivergedFromExpected?: boolean;
	ownerChildrenPreservationTarget?: ArrayPropertyState<Manageable<any>>;
	observedOwners: Set<Manageable<any>>;
	childCandidates: Set<Manageable<any>>;
};

type InitializationRollbackBaseline = {
	parents: ArrayPropertyBaseline<Manageable<any> | undefined>;
	children: ArrayPropertyBaseline<Manageable<any>>;
	parent?: Manageable<any>;
	parentChildren?: ArrayPropertyBaseline<Manageable<any>>;
	parentChildrenWorkingReference?: Manageable<any>[];
	parentChildrenWorkingSnapshot?: ArrayPropertyBaseline<Manageable<any>>;
};

type OpeningReservationGroup = {
	participantReferences: Map<Manageable<any>, number>;
	reservations: Set<OpeningReservation<any>>;
};

type OpeningReservation<T extends Manageable<any>> = {
	promise: Promise<T>;
	phase: "opening" | "rollback";
	observedReservations: Set<OpeningReservation<T>>;
	adoptedParents: Set<Manageable<any>>;
	contributedParticipants: Set<Manageable<any>>;
	wakeWaiters: Set<() => void>;
	group: OpeningReservationGroup;
	rootProgram?: Manageable<any>;
	programsByAddress: Map<string, Set<Manageable<any>>>;
	programs: Set<Manageable<any>>;
	provisionalTerminalStates: Set<TerminalOperationState<T>>;
};

export type EventOptions = {
	onBeforeOpen?: (program: Manageable<any>) => Promise<void> | void;
	onOpen?: (program: Manageable<any>) => Promise<void> | void;
	onDrop?: (program: Manageable<any>) => Promise<void> | void;
	onClose?: (program: Manageable<any>) => Promise<void> | void;
};

export type OpenOptions<T extends Manageable<ExtractArgs<T>>> = {
	timeout?: number;
	existing?: ProgramMergeStrategy;
	mode?: "auto" | "local" | "canonical";
} & ProgramInitializationOptions<ExtractArgs<T>, T>;

export type WithArgs<Args> = { args?: Args };
export type WithParent<T> = { parent?: T };
export type Closeable = { closed: boolean; close(): Promise<boolean> };
type WithNode = { node: { identity: Identity } };
export type Addressable = {
	address: Address;
};
export interface Saveable {
	save(
		store: Blocks,
		options?: {
			format?: string;
			timeout?: number;
			skipOnAddress?: boolean;
			save?: (address: string) => boolean | Promise<boolean>;
		},
	): Promise<Address>;

	delete(): Promise<void>;
}
export type CanEmit = { emitEvent: (event: CustomEvent) => void };

export type CanOpen<Args> = {
	beforeOpen: (client: any, options?: EventOptions) => Promise<void>;
	open(args?: Args): Promise<void>;
	afterOpen: () => Promise<void>;
};

export type Manageable<Args> = Closeable &
	Addressable &
	CanOpen<Args> &
	Saveable &
	CanEmit & {
		children: Manageable<any>[];
		parents: (Manageable<any> | undefined)[];
		/**
		 * Programs with terminal work that must happen before `Program.end()` can
		 * fence parent reuse while that work is in progress.
		 */
		readonly acceptsParentAttachments?: boolean;
		/** Base terminal tail/deletion that must be resumed with this operation. */
		readonly pendingTerminalOperation?: TerminalOperation;
		/** A lifecycle callback must not recursively wait for Handler.stop(). */
		readonly terminalLifecycleCallbackRunning?: boolean;
		[TERMINAL_BASE_CHECKPOINT]?: () => number;
		[TERMINAL_BASE_COMMIT]?: (
			afterVersion: number,
			type: TerminalOperation,
			from?: Manageable<any>,
		) => TerminalBaseCommit | undefined;
		[TERMINAL_BASE_RETRY]?: (
			commit: TerminalBaseCommit,
			operation: () => Promise<boolean>,
		) => Promise<boolean>;
		[TERMINAL_OUTER_CLEANUP_RETAIN]?: () => object;
		[TERMINAL_OUTER_CLEANUP_RELEASE]?: (lease: object) => void;
	} & WithNode;

export type ProgramInitializationOptions<Args, T extends Manageable<Args>> = {
	// TODO
	// reset: boolean
} & WithArgs<Args> &
	WithParent<T> &
	EventOptions;

type HandlerTerminalProtocol = {
	supports: (program: Manageable<any>) => boolean;
	closed: (program: Manageable<any>) => boolean;
	checkpoint: (program: Manageable<any>) => number;
	commit: (
		program: Manageable<any>,
		afterVersion: number,
		type: TerminalOperation,
		from?: Manageable<any>,
	) => TerminalBaseCommit | undefined;
	retry: (
		program: Manageable<any>,
		commit: TerminalBaseCommit,
		operation: () => Promise<boolean>,
	) => Promise<boolean>;
	retainCleanup: (program: Manageable<any>) => object;
	releaseCleanup: (program: Manageable<any>, lease: object) => void;
};

type HandlerProperties<T extends Manageable<any>> = {
	client: { services: { blocks: Blocks }; stop: () => Promise<void> };
	load: (
		address: Address,
		blocks: Blocks,
		options?: { timeout?: number },
	) => Promise<T | undefined>;
	shouldMonitor: (thing: any) => boolean;
	identity: Identity;
	getDependencies?: (program: T) => Manageable<any>[];
};

type HandlerConstructorProperties<T extends Manageable<any>> =
	HandlerProperties<T> & {
		terminalProtocol?: HandlerTerminalProtocol;
	};

// Terminal proof dispatch is a trust boundary. Keep both the canonical callback
// table and the support cache in module-private state: Programs can reach their
// public Peerbit Handler, so TypeScript-private fields or public constructor
// properties would remain writable at runtime between checkpoint and commit.
const handlerTerminalProtocols = new WeakMap<
	object,
	Readonly<HandlerTerminalProtocol>
>();
const handlerTerminalProtocolSupport = new WeakMap<
	object,
	WeakMap<Manageable<any>, boolean>
>();
const monitoredTerminalPrograms = new WeakMap<object, Manageable<any>>();

const supportsTerminalBaseCommitProof = (
	handler: object,
	program: Manageable<any>,
): boolean => {
	let support = handlerTerminalProtocolSupport.get(handler);
	if (!support) {
		support = new WeakMap();
		handlerTerminalProtocolSupport.set(handler, support);
	}
	const cached = support.get(program);
	if (cached != null) return cached;
	const supports =
		handlerTerminalProtocols.get(handler)?.supports(program) === true;
	support.set(program, supports);
	return supports;
};

const terminalCheckpoint = (
	handler: object,
	program: Manageable<any>,
): number => {
	if (!supportsTerminalBaseCommitProof(handler, program)) return 0;
	return handlerTerminalProtocols.get(handler)?.checkpoint(program) ?? 0;
};

const terminalClosed = (handler: object, program: Manageable<any>): boolean => {
	if (!supportsTerminalBaseCommitProof(handler, program)) return program.closed;
	return handlerTerminalProtocols.get(handler)!.closed(program);
};

const terminalCommit = (
	handler: object,
	program: Manageable<any>,
	checkpoint: number,
	type: TerminalOperation,
	from?: Manageable<any>,
): TerminalBaseCommit | undefined => {
	if (!supportsTerminalBaseCommitProof(handler, program)) return undefined;
	return handlerTerminalProtocols
		.get(handler)
		?.commit(program, checkpoint, type, from);
};

const retryCommittedTerminalCall = (
	handler: object,
	program: Manageable<any>,
	commit: TerminalBaseCommit,
	operation: () => Promise<boolean>,
): Promise<boolean> => {
	if (!supportsTerminalBaseCommitProof(handler, program)) return operation();
	return (
		handlerTerminalProtocols.get(handler)?.retry(program, commit, operation) ??
		operation()
	);
};

const retainTerminalCleanupLease = (
	handler: object,
	program: Manageable<any>,
): object | undefined => {
	if (!supportsTerminalBaseCommitProof(handler, program)) return undefined;
	return handlerTerminalProtocols.get(handler)?.retainCleanup(program);
};

const releaseTerminalCleanupLease = (
	handler: object,
	program: Manageable<any>,
	lease: object | undefined,
): void => {
	if (!lease || !supportsTerminalBaseCommitProof(handler, program)) return;
	handlerTerminalProtocols.get(handler)?.releaseCleanup(program, lease);
};

const assertCanAttachParent = (child: Manageable<any>) => {
	if (child.acceptsParentAttachments === false) {
		throw new Error("Program is terminating and cannot accept another parent");
	}
};

export const addParent = (child: Manageable<any>, parent?: Manageable<any>) => {
	assertCanAttachParent(child);
	if (child.parents && child.parents.includes(parent) && parent == null) {
		return; // prevent root parents to exist multiple times. This will allow use to close a program onces even if it is reused multiple times
	}

	(child.parents || (child.parents = [])).push(parent);
	if (parent) {
		(parent.children || (parent.children = [])).push(child);
	}
};

/**
 * Owns lifecycle state for one client. Handler instances do not share address
 * reservations, so a client must use a single Handler for a program graph.
 */
export class Handler<T extends Manageable<any>> {
	readonly properties: HandlerProperties<T>;
	items: Map<string, T>;
	private _openQueue: Map<string, PQueue>;
	private _openingPromises: Map<string, Promise<T>>;
	private _openingReservations: Set<OpeningReservation<T>>;
	private _openingReservationsByPromise: WeakMap<
		Promise<T>,
		OpeningReservation<T>
	>;
	private _openingReservationsByAddress: Map<
		string,
		Set<OpeningReservation<T>>
	>;
	private _parentAttachmentReservations: Map<Manageable<any>, number>;
	private _openAdmissions: Set<Promise<void>>;
	private _openLifecycleCallbacks: number;
	private _terminalLifecycleCallbacks: number;
	private _lifecycleMethodInvocations: number;
	private _acceptingOpens: boolean;
	private _stopPromise?: Promise<void>;
	private _lifecycleState: HandlerLifecycleState;
	private _failedInitializations: Map<string, T>;
	private _cleanupResiduals: Map<Manageable<any>, CleanupResidual>;
	private _terminalOperationsByProgram: WeakMap<
		Manageable<any>,
		TerminalOperationState<T>
	>;
	private _terminalOperationsByAddress: Map<Address, TerminalOperationState<T>>;
	private _initializationRollbacks: WeakSet<Manageable<any>>;
	private _initializationRollbackOwners: Map<
		Manageable<any>,
		InitializationRollbackOwner
	>;
	private _replacementClosures: WeakSet<Manageable<any>>;

	constructor(properties: HandlerConstructorProperties<T>) {
		const { terminalProtocol, ...publicProperties } = properties;
		this.properties = publicProperties;
		if (terminalProtocol) {
			handlerTerminalProtocols.set(
				this,
				Object.freeze({ ...terminalProtocol }),
			);
		}
		this._openQueue = new Map();
		this._openingPromises = new Map();
		this._openingReservations = new Set();
		this._openingReservationsByPromise = new WeakMap();
		this._openingReservationsByAddress = new Map();
		this._parentAttachmentReservations = new Map();
		this._openAdmissions = new Set();
		this._openLifecycleCallbacks = 0;
		this._terminalLifecycleCallbacks = 0;
		this._lifecycleMethodInvocations = 0;
		this._acceptingOpens = true;
		this._lifecycleState = "running";
		this._failedInitializations = new Map();
		this._cleanupResiduals = new Map();
		this._terminalOperationsByProgram = new WeakMap();
		this._terminalOperationsByAddress = new Map();
		this._initializationRollbacks = new WeakSet();
		this._initializationRollbackOwners = new Map();
		this._replacementClosures = new WeakSet();
		this.items = new Map();
	}

	/**
	 * Re-open admissions after the owning client has finished starting all of the
	 * services programs depend on. This is an internal client lifecycle hook, not
	 * a substitute for starting the client itself.
	 *
	 * This is called by the owning client after its services have started.
	 */
	start(): void {
		if (this._lifecycleState === "running") {
			return;
		}
		this.assertCanStart();
		this._lifecycleState = "running";
		this._acceptingOpens = true;
	}

	/** Validate restart eligibility without opening admissions. */
	assertCanStart(): void {
		if (this._lifecycleState === "running") {
			return;
		}
		if (
			this._lifecycleState !== "stopped" ||
			this._stopPromise ||
			this.items.size > 0 ||
			this._openingPromises.size > 0 ||
			this._openingReservations.size > 0 ||
			this._openingReservationsByAddress.size > 0 ||
			this._parentAttachmentReservations.size > 0 ||
			this._openAdmissions.size > 0 ||
			this._failedInitializations.size > 0 ||
			this._cleanupResiduals.size > 0 ||
			this._initializationRollbackOwners.size > 0 ||
			this._terminalOperationsByAddress.size > 0
		) {
			throw new Error(
				"Program handler cannot start before a successful stop has fully drained",
			);
		}
	}

	/**
	 * Stop the handler after admitted lifecycle work drains.
	 *
	 * Lifecycle overrides and callbacks must not await their own handler/client
	 * stop. Immediate reentry is rejected. After user code has yielded, portable
	 * JavaScript runtimes provide no reliable async-call provenance with which to
	 * distinguish self-reentry from an unrelated external stop; schedule stop from
	 * the lifecycle owner instead. A conservatively rejected external stop remains
	 * retryable after the active callback finishes.
	 */
	stop(): Promise<void> {
		if (
			this._openLifecycleCallbacks > 0 ||
			this._terminalLifecycleCallbacks > 0 ||
			this._lifecycleMethodInvocations > 0 ||
			[...this.items.values()].some(
				(program) => program.terminalLifecycleCallbackRunning === true,
			)
		) {
			return Promise.reject(
				new TerminalOperationNotStartedError(
					"Program lifecycle callbacks cannot wait for their own handler to stop",
				),
			);
		}
		if (this._stopPromise) {
			return this._stopPromise;
		}
		if (this._lifecycleState === "stopped") {
			return Promise.resolve();
		}
		// Close admissions synchronously. Every open that passed this fence has a
		// completion promise in `_openAdmissions`, so stop has a stable set to drain.
		this._acceptingOpens = false;
		this._lifecycleState = "stopping";
		this._stopPromise = this.stopOnce().then(
			() => {
				// The client still has storage, index, and transport teardown to perform.
				// Only its completed start() transition may re-open admissions.
				this._lifecycleState = "stopped";
				this._stopPromise = undefined;
			},
			(error) => {
				// Keep admissions closed after a partial stop. Clearing only the promise
				// makes stop() retryable without exposing the live residual items to opens.
				this._lifecycleState = "failed";
				this._stopPromise = undefined;
				throw error;
			},
		);
		return this._stopPromise;
	}

	/**
	 * Async methods execute synchronously until their first await. Fence that
	 * invocation window so a lifecycle override cannot make stop wait on the very
	 * operation that is awaiting stop. Awaited user callbacks have their own wider
	 * callback fences below.
	 */
	private invokeLifecycleMethod<R>(
		operation: () => R | PromiseLike<R>,
	): Promise<R> {
		this._lifecycleMethodInvocations += 1;
		try {
			return Promise.resolve(operation());
		} finally {
			this._lifecycleMethodInvocations -= 1;
		}
	}

	/**
	 * Program.end() can only observe the base implementation's lifetime. A
	 * subclass is still free to do asynchronous cleanup after `super.close()` or
	 * `super.drop()` has returned. Wrap the public, dynamically-dispatched method
	 * on every managed instance so Handler state covers that full outer promise.
	 */
	private monitorTerminalOperations(
		address: Address,
		program: T,
	): { state: TerminalOperationState<T>; newLifecycle: boolean } {
		let state = this._terminalOperationsByProgram.get(program);
		const newLifecycle =
			!state || this._terminalOperationsByAddress.get(address) !== state;
		if (!state) {
			state = {
				program,
				address,
				activeCalls: 0,
				pending: new Set(),
				failed: false,
				terminalCompleted: false,
				outerTail: Promise.resolve(),
				outerCalls: new Set(),
			};
			monitoredTerminalPrograms.set(state, program);
			this._terminalOperationsByProgram.set(program, state);
		}

		state.address = address;
		if (newLifecycle) {
			state.failed = false;
			state.failedOperation = undefined;
			state.failedCall = undefined;
			state.terminalCompleted = false;
		}
		this._terminalOperationsByAddress.set(address, state);
		this.refreshTerminalOperationWrappers(state);
		return { state, newLifecycle };
	}

	private refreshTerminalOperationWrappers(
		state: TerminalOperationState<T>,
	): void {
		const program = state.program;
		if (program.close !== state.closeWrapper) {
			const close = program.close;
			state.closeOperation = close;
			const closeWrapper = (...args: any[]) =>
				this.#runTerminalOperation(state, "close", close, args, closeWrapper);
			state.closeWrapper = closeWrapper;
			program.close = closeWrapper;
		}

		const droppable = program as T & {
			drop?: (...args: any[]) => Promise<boolean>;
		};
		if (
			typeof droppable.drop === "function" &&
			droppable.drop !== state.dropWrapper
		) {
			const drop = droppable.drop;
			state.dropOperation = drop;
			const dropWrapper = (...args: any[]) =>
				this.#runTerminalOperation(state, "drop", drop, args, dropWrapper);
			state.dropWrapper = dropWrapper;
			droppable.drop = dropWrapper;
		}
	}

	#runTerminalOperation(
		state: TerminalOperationState<T>,
		type: TerminalOperation,
		operation: (...args: any[]) => Promise<boolean>,
		args: any[],
		invokedWrapper: (...args: any[]) => Promise<boolean>,
	): Promise<boolean> {
		const program = monitoredTerminalPrograms.get(state);
		if (!program) {
			return Promise.reject(
				new Error(
					"Program terminal operation has no trusted monitored identity",
				),
			);
		}
		const from = args[0] as Manageable<any> | undefined;
		const synchronousInvocation = state.synchronouslyInvokingWrapper;
		if (synchronousInvocation) {
			if (type !== synchronousInvocation.type) {
				return Promise.reject(
					new TerminalOperationNotStartedError(
						"Program terminal methods cannot synchronously invoke another terminal operation",
					),
				);
			}
			if (!this.sameTerminalArgs(args, synchronousInvocation.args)) {
				return Promise.reject(
					new TerminalOperationNotStartedError(
						"A captured terminal wrapper cannot change the active operation owner",
					),
				);
			}
			if (invokedWrapper !== synchronousInvocation.wrapper) {
				// A post-open replacement may delegate to the Handler wrapper it
				// captured before being re-wrapped. Peel that stale wrapper back to its
				// raw operation; the current outer wrapper still monitors the full promise.
				try {
					return Promise.resolve(operation.apply(program, args));
				} catch (error) {
					return Promise.reject(error);
				}
			}
			return Promise.reject(
				new TerminalOperationNotStartedError(
					"Program terminal methods cannot wait for their own active operation",
				),
			);
		}
		if (
			program.terminalLifecycleCallbackRunning ||
			(this._terminalLifecycleCallbacks > 0 && state.activeCalls > 0)
		) {
			return Promise.reject(
				new TerminalOperationNotStartedError(
					"Program lifecycle callbacks cannot wait for their own terminal operation",
				),
			);
		}
		const parentIndex =
			program.parents?.findIndex((parent) => parent === from) ?? -1;
		const terminal =
			terminalClosed(this, program) ||
			parentIndex === -1 ||
			(program.parents?.length ?? 0) === 1;
		const currentWrapper =
			type === "close" ? state.closeWrapper : state.dropWrapper;
		if (
			currentWrapper &&
			invokedWrapper !== currentWrapper &&
			[...state.outerCalls].some(
				(call) => call.type === type && call.invokedWrapper === currentWrapper,
			)
		) {
			// Once replacement code has yielded there is no portable async-call
			// context that can distinguish its captured stale wrapper from an
			// unrelated external stale call. Never return the active outer promise to
			// code that may itself be awaiting this call: reject before base progress
			// instead of forming a self-cycle that wedges Handler.stop().
			return Promise.reject(
				new TerminalOperationNotStartedError(
					"A stale terminal wrapper cannot join its active replacement after yielding",
				),
			);
		}
		const matching = [...state.outerCalls].find(
			(call) =>
				call.terminal &&
				call.from === from &&
				(call.type === type || (type === "close" && call.type === "drop")),
		);
		if (matching) {
			return matching.promise;
		}
		try {
			this.assertTerminalGraphCanStart(program);
		} catch (error) {
			return Promise.reject(error);
		}

		// The outer wrapper serializes subclass methods before Program sees them. A
		// call admitted while another owner is still queued can therefore look
		// non-terminal now even though it will be the final release when it runs.
		// Count already-admitted compatible calls against the owner's current
		// references so a duplicate of that future terminal call shares its exact
		// promise. Keep one distinct call per real duplicate ownership reference.
		const compatibleCalls = [...state.outerCalls].filter(
			(call) =>
				call.from === from &&
				(call.type === type || (type === "close" && call.type === "drop")),
		);
		const ownerReferences =
			program.parents?.filter((parent) => parent === from).length ?? 0;
		const reservedOwnerReferences = compatibleCalls.filter(
			(call) => call.claimedOwnerReference,
		).length;
		const synchronouslyReleasedOwnerReferences = compatibleCalls.reduce(
			(released, call) => {
				const before = call.ownerReferencesBeforeInvoke;
				const after = call.ownerReferencesAfterInvoke;
				return (
					released +
					(before == null || after == null
						? 0
						: Math.min(1, Math.max(0, before - after)))
				);
			},
			0,
		);
		const unclaimedOwnerReferences =
			ownerReferences -
			(reservedOwnerReferences - synchronouslyReleasedOwnerReferences);
		if (compatibleCalls.length > 0 && unclaimedOwnerReferences <= 0) {
			return compatibleCalls.at(-1)!.promise;
		}

		let resolve!: (value: boolean) => void;
		let reject!: (reason?: unknown) => void;
		const tracked = new Promise<boolean>((promiseResolve, promiseReject) => {
			resolve = promiseResolve;
			reject = promiseReject;
		});
		const outerCall: OuterTerminalCall = {
			type,
			from,
			terminal,
			claimedOwnerReference: ownerReferences > 0,
			invokedWrapper,
			promise: tracked,
		};
		const hasPredecessor = state.outerCalls.size > 0;
		const terminalCompletedBeforeCall = state.terminalCompleted;
		state.outerCalls.add(outerCall);
		state.activeCalls += 1;
		state.terminalCompleted = false;
		this._terminalOperationsByAddress.set(state.address, state);
		state.pending.add(tracked);

		const predecessor = state.outerTail;
		const execute = async (): Promise<boolean> => {
			const recovering = state.failed;
			const failedCall = state.failedCall;
			if (
				recovering &&
				(failedCall?.type !== type ||
					!this.sameTerminalArgs(failedCall.args, args))
			) {
				throw new Error(
					`Program at ${state.address} has failed terminal cleanup that must be retried first`,
				);
			}

			const startedClosed = terminalClosed(this, program);
			const startedAcceptingParents = program.acceptsParentAttachments;
			const startedParentsState = this.captureArrayPropertyState<
				Manageable<any> | undefined
			>(program, "parents");
			const startedParents = [...(startedParentsState.value ?? [])];
			const startedParentChildReferences = new Map<Manageable<any>, number>();
			for (const parent of startedParents) {
				if (parent == null || startedParentChildReferences.has(parent))
					continue;
				startedParentChildReferences.set(
					parent,
					parent.children?.filter((child) => child === program).length ?? 0,
				);
			}
			const checkpoint = terminalCheckpoint(this, program);
			const existingCleanupLease = state.cleanupLease;
			if (!state.cleanupLease) {
				state.cleanupLease = retainTerminalCleanupLease(this, program);
			}
			const releaseProvisionalCleanupLease = () => {
				if (!existingCleanupLease && state.cleanupLease) {
					releaseTerminalCleanupLease(this, program, state.cleanupLease);
					state.cleanupLease = undefined;
				}
			};
			let rejectedUncommittedTerminalResult = false;
			try {
				const invoke = () => {
					outerCall.ownerReferencesBeforeInvoke =
						program.parents?.filter((parent) => parent === from).length ?? 0;
					let operationResult: Promise<boolean>;
					const previousSynchronouslyInvokingWrapper =
						state.synchronouslyInvokingWrapper;
					state.synchronouslyInvokingWrapper = {
						type,
						wrapper: invokedWrapper,
						args,
					};
					try {
						operationResult = this.invokeLifecycleMethod(() =>
							operation.apply(program, args),
						);
					} finally {
						state.synchronouslyInvokingWrapper =
							previousSynchronouslyInvokingWrapper;
						outerCall.ownerReferencesAfterInvoke =
							program.parents?.filter((parent) => parent === from).length ?? 0;
					}
					return operationResult!;
				};
				const result =
					recovering && failedCall?.commit
						? await retryCommittedTerminalCall(
								this,
								program,
								failedCall.commit,
								invoke,
							)
						: await invoke();
				const completionCommit =
					failedCall?.commit ||
					terminalCommit(this, program, checkpoint, type, from);
				const supportsBaseCommitProof = supportsTerminalBaseCommitProof(
					this,
					program,
				);
				const observedReleasedParentReferences = Math.max(
					0,
					startedParents.filter((parent) => parent === from).length -
						(program.parents?.filter((parent) => parent === from).length ?? 0),
				);
				if (
					supportsBaseCommitProof &&
					(recovering || !startedClosed || !terminalCompletedBeforeCall) &&
					completionCommit == null
				) {
					rejectedUncommittedTerminalResult = true;
					throw new Error(
						`Program at ${state.address} reported ${type} success without reaching its base terminal operation`,
					);
				}
				if (
					completionCommit != null &&
					(completionCommit.result !== result ||
						terminalClosed(this, program) !== completionCommit.result)
				) {
					throw new Error(
						`Program at ${state.address} did not preserve the exact base terminal result`,
					);
				}
				if (recovering) {
					const recoveryOwner = failedCall?.args[0] as
						| Manageable<any>
						| undefined;
					const releasedDuringRetry = Math.max(
						0,
						startedParents.filter((parent) => parent === recoveryOwner).length -
							(program.parents?.filter((parent) => parent === recoveryOwner)
								.length ?? 0),
					);
					const rollbackOwner = this._initializationRollbackOwners.get(program);
					const retryingCommittedInitializationRollback =
						rollbackOwner?.phase === "committed" &&
						rollbackOwner.owner === recoveryOwner &&
						failedCall?.type === "close";
					this.removeReleasedParentChildReferences(
						program,
						recoveryOwner,
						retryingCommittedInitializationRollback
							? releasedDuringRetry
							: Math.max(
									failedCall?.commit?.releasedParentReferences ??
										failedCall?.releasedParentReferences ??
										0,
									releasedDuringRetry,
								),
					);
					state.failed = false;
					state.failedOperation = undefined;
					state.failedCall = undefined;
					this.releaseRetainedTerminalIdentity(state);
				} else {
					this.removeReleasedParentChildReferences(
						program,
						from,
						completionCommit?.releasedParentReferences ??
							(supportsBaseCommitProof ? 0 : observedReleasedParentReferences),
					);
				}
				if (result && terminalClosed(this, program) && !state.failed) {
					state.terminalCompleted = true;
				}
				if (state.cleanupLease) {
					releaseTerminalCleanupLease(this, program, state.cleanupLease);
					state.cleanupLease = undefined;
				}
				return result;
			} catch (error) {
				const observedCommit =
					failedCall?.commit ||
					terminalCommit(this, program, checkpoint, type, from);
				if (rejectedUncommittedTerminalResult && observedCommit == null) {
					const observedParents = [...(program.parents ?? [])];
					this.restoreUncommittedParentReferences(program, startedParentsState);
					this.restoreUncommittedParentChildReferences(
						program,
						startedParentChildReferences,
						observedParents,
					);
				}
				if (
					!recovering &&
					error instanceof TerminalOperationNotStartedError &&
					!observedCommit
				) {
					// Exclude this call's provisional lease while validating that the
					// explicit precondition error made no lifecycle mutation.
					releaseProvisionalCleanupLease();
					if (
						terminalClosed(this, program) === startedClosed &&
						program.acceptsParentAttachments === startedAcceptingParents &&
						this.sameParents(program.parents ?? [], startedParents)
					) {
						state.terminalCompleted = startedClosed;
						throw error;
					}
					state.cleanupLease = retainTerminalCleanupLease(this, program);
				}
				if (startedClosed && !recovering) {
					// A fresh drop of an already-cleanly-closed program, or a drop queued
					// behind a winning close, is an API rejection rather than failed cleanup.
					state.terminalCompleted = true;
					releaseProvisionalCleanupLease();
					throw error;
				}
				const commit = observedCommit;
				const baseProgressed =
					commit != null ||
					terminalClosed(this, program) !== startedClosed ||
					!this.sameParents(program.parents ?? [], startedParents);
				const recoveryType: TerminalOperation =
					type === "drop" && !baseProgressed
						? "drop"
						: commit || terminalClosed(this, program)
							? type
							: "close";
				const releasedParentReferences = Math.max(
					failedCall?.releasedParentReferences ?? 0,
					startedParents.filter((parent) => parent === from).length -
						(program.parents?.filter((parent) => parent === from).length ?? 0),
				);
				state.failed = true;
				state.failedOperation = recoveryType;
				state.failedCall = {
					type: recoveryType,
					args: [...args],
					commit: recoveryType === type ? commit : undefined,
					releasedParentReferences,
				};
				state.terminalCompleted = false;
				const monitored = this.items.get(state.address);
				if (!monitored || monitored === program) {
					this.items.set(state.address, program as T);
				} else {
					// `items` is address-keyed, but embedded opening graphs may contain a
					// distinct instance with the same address as a monitored root. Never
					// orphan that root by replacing it with this failed cleanup identity.
					const residual = this.reserveCleanupResidual(program);
					residual.terminalState = state;
				}
				this._terminalOperationsByAddress.set(state.address, state);
				throw error;
			}
		};

		// Invoke the first outer method immediately. Program.end() and subclasses
		// establish their mutation/attachment fences synchronously before their
		// first await; deferring through Promise.then would admit same-turn writes
		// after close/drop had already been requested. Only true followers use the
		// serialized promise tail.
		const execution = hasPredecessor
			? predecessor.then(execute, execute)
			: execute();
		state.outerTail = tracked.then(
			(): void => undefined,
			(): void => undefined,
		);
		void execution.then(resolve, reject);
		void tracked.then(
			() => this.finishOuterTerminalCall(state, outerCall, tracked),
			() => this.finishOuterTerminalCall(state, outerCall, tracked),
		);
		logger.trace(
			`Tracking outer ${type} operation for program at ${state.address}`,
		);
		return tracked;
	}

	private finishOuterTerminalCall(
		state: TerminalOperationState<T>,
		call: OuterTerminalCall,
		promise: Promise<boolean>,
	): void {
		state.activeCalls -= 1;
		state.outerCalls.delete(call);
		state.pending.delete(promise);
		this.finishTerminalOperation(state);
	}

	private sameTerminalArgs(left: any[], right: any[]): boolean {
		return (
			left.length === right.length &&
			left.every((value, index) => value === right[index])
		);
	}

	private sameParents(
		left: (Manageable<any> | undefined)[],
		right: (Manageable<any> | undefined)[],
	): boolean {
		return (
			left.length === right.length &&
			left.every((parent, index) => parent === right[index])
		);
	}

	private captureArrayProperty<V>(
		target: object,
		property: "parents" | "children",
	): ArrayPropertyBaseline<V> {
		const value = (target as Record<"parents" | "children", V[] | undefined>)[
			property
		];
		return {
			hadOwnProperty: Object.prototype.hasOwnProperty.call(target, property),
			value: value == null ? undefined : [...value],
		};
	}

	private captureArrayPropertyState<V>(
		target: object,
		property: "parents" | "children",
	): ArrayPropertyState<V> {
		return {
			...this.captureArrayProperty<V>(target, property),
			reference: (target as Record<"parents" | "children", V[] | undefined>)[
				property
			],
		};
	}

	private restoreArrayProperty<V>(
		target: object,
		property: "parents" | "children",
		baseline: ArrayPropertyBaseline<V>,
	): void {
		if (!baseline.hadOwnProperty) {
			delete (
				target as Partial<Record<"parents" | "children", V[] | undefined>>
			)[property];
			return;
		}
		(target as Record<"parents" | "children", V[] | undefined>)[property] =
			baseline.value == null ? undefined : [...baseline.value];
	}

	private rollbackOwnerChildrenMatchWorkingSnapshot(
		rollback: InitializationRollbackOwner,
	): boolean {
		const owner = rollback.owner;
		const snapshot = rollback.ownerChildrenWorkingSnapshot;
		if (!owner || !snapshot) return true;
		const currentReference = owner.children;
		const currentValues = currentReference;
		const expectedValues = snapshot.value;
		return (
			Object.prototype.hasOwnProperty.call(owner, "children") ===
				snapshot.hadOwnProperty &&
			currentReference === rollback.ownerChildrenWorkingReference &&
			((currentValues == null && expectedValues == null) ||
				(currentValues != null &&
					expectedValues != null &&
					currentValues.length === expectedValues.length &&
					currentValues.every(
						(child, index) => child === expectedValues[index],
					)))
		);
	}

	private updateRollbackOwnerChildrenWorkingSnapshot(
		rollback: InitializationRollbackOwner,
	): void {
		const owner = rollback.owner;
		if (!owner) return;
		const state = this.captureArrayPropertyState<Manageable<any>>(
			owner,
			"children",
		);
		rollback.ownerChildrenWorkingReference = state.reference;
		rollback.ownerChildrenWorkingSnapshot = {
			hadOwnProperty: state.hadOwnProperty,
			value: state.value,
		};
	}

	private adoptExactGeneratedRollbackOwnerChildrenReference(
		rollback: InitializationRollbackOwner,
	): void {
		const owner = rollback.owner;
		const snapshot = rollback.ownerChildrenWorkingSnapshot;
		if (!owner || !snapshot || rollback.ownerChildrenWorkingReference != null) {
			return;
		}
		const current = owner.children;
		const expected = snapshot.value;
		if (
			!snapshot.hadOwnProperty ||
			!Object.prototype.hasOwnProperty.call(owner, "children") ||
			current == null ||
			expected == null ||
			current.length !== expected.length ||
			!current.every((child, index) => child === expected[index])
		) {
			return;
		}
		// An absent/undefined baseline forces Program.beforeOpen() to allocate the
		// expected owner array before opening nested programs. If nested open fails
		// before Handler's callback, adopt that exact generated reference only when
		// its full shape still matches the one Handler created.
		rollback.ownerChildrenWorkingReference = current;
	}

	private recordRollbackOwnerChildrenDivergence(
		rollback: InitializationRollbackOwner,
	): void {
		const owner = rollback.owner;
		if (!owner || this.rollbackOwnerChildrenMatchWorkingSnapshot(rollback)) {
			return;
		}
		rollback.ownerChildrenDivergedFromExpected = true;
		rollback.ownerChildrenPreservationTarget = this.captureArrayPropertyState<
			Manageable<any>
		>(owner, "children");
	}

	private recordRollbackOwnerChildrenIdentityDivergence(
		rollback: InitializationRollbackOwner,
	): void {
		const owner = rollback.owner;
		const snapshot = rollback.ownerChildrenWorkingSnapshot;
		if (!owner || !snapshot) return;
		const currentReference = owner.children;
		const currentHadOwnProperty = Object.prototype.hasOwnProperty.call(
			owner,
			"children",
		);
		const preservationTarget = rollback.ownerChildrenPreservationTarget;
		const preservationTargetIsCurrent =
			preservationTarget == null ||
			(preservationTarget.hadOwnProperty === currentHadOwnProperty &&
				preservationTarget.reference === currentReference);
		if (
			currentHadOwnProperty === snapshot.hadOwnProperty &&
			currentReference === rollback.ownerChildrenWorkingReference &&
			preservationTargetIsCurrent
		) {
			return;
		}
		rollback.ownerChildrenDivergedFromExpected = true;
		rollback.ownerChildrenPreservationTarget = this.captureArrayPropertyState<
			Manageable<any>
		>(owner, "children");
	}

	/**
	 * Restore the failed generation's inverse edge count without discarding
	 * unrelated children attached to the owner while the open was in flight.
	 */
	private restoreRollbackOwnerChildren(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): void {
		const owner = rollback.owner;
		const baseline = rollback.ownerChildren;
		if (!owner || !baseline) return;

		const baselineValues = baseline.value ?? [];
		const currentHadOwnProperty = Object.prototype.hasOwnProperty.call(
			owner,
			"children",
		);
		const currentReference = owner.children;
		const currentValues = currentReference ?? [];
		const currentUnrelated = currentValues.filter((child) => child !== program);
		const baselineUnrelated = baselineValues.filter(
			(child) => child !== program,
		);
		const unrelatedChildrenChanged =
			currentUnrelated.length !== baselineUnrelated.length ||
			currentUnrelated.some(
				(child, index) => child !== baselineUnrelated[index],
			);
		const ownerPropertyChanged =
			rollback.ownerChildrenDivergedFromExpected === true ||
			unrelatedChildrenChanged ||
			!currentHadOwnProperty ||
			currentReference == null ||
			(rollback.ownerChildrenWorkingReference != null &&
				currentReference !== rollback.ownerChildrenWorkingReference);

		// Match the surviving baseline siblings into the current unrelated sequence.
		// Candidate occurrences can then be restored around those anchors without
		// reordering, deleting, or resurrecting any unrelated child.
		const currentIndicesByChild = new Map<Manageable<any>, number[]>();
		for (let index = 0; index < currentUnrelated.length; index++) {
			const child = currentUnrelated[index]!;
			const indices = currentIndicesByChild.get(child) ?? [];
			indices.push(index);
			currentIndicesByChild.set(child, indices);
		}
		const cursorsByChild = new Map<Manageable<any>, number>();
		const matchedCurrentIndexByBaselineIndex = new Map<number, number>();
		let lastMatchedCurrentIndex = -1;
		for (let index = 0; index < baselineValues.length; index++) {
			const child = baselineValues[index]!;
			if (child === program) continue;
			const indices = currentIndicesByChild.get(child);
			if (!indices) continue;
			let cursor = cursorsByChild.get(child) ?? 0;
			while (
				cursor < indices.length &&
				indices[cursor]! <= lastMatchedCurrentIndex
			) {
				cursor += 1;
			}
			cursorsByChild.set(child, cursor + 1);
			const currentIndex = indices[cursor];
			if (currentIndex == null) continue;
			matchedCurrentIndexByBaselineIndex.set(index, currentIndex);
			lastMatchedCurrentIndex = currentIndex;
		}

		const previousAnchorByBaselineIndex: Array<number | undefined> = [];
		let previousAnchor: number | undefined;
		for (let index = 0; index < baselineValues.length; index++) {
			previousAnchorByBaselineIndex[index] = previousAnchor;
			previousAnchor =
				matchedCurrentIndexByBaselineIndex.get(index) ?? previousAnchor;
		}
		const nextAnchorByBaselineIndex: Array<number | undefined> = [];
		let nextAnchor: number | undefined;
		for (let index = baselineValues.length - 1; index >= 0; index--) {
			nextAnchorByBaselineIndex[index] = nextAnchor;
			nextAnchor = matchedCurrentIndexByBaselineIndex.get(index) ?? nextAnchor;
		}

		const candidateOccurrencesBySlot = new Array<number>(
			currentUnrelated.length + 1,
		).fill(0);
		for (let index = 0; index < baselineValues.length; index++) {
			if (baselineValues[index] !== program) continue;
			const previous = previousAnchorByBaselineIndex[index];
			const next = nextAnchorByBaselineIndex[index];
			const slot = previous == null ? (next ?? 0) : previous + 1;
			candidateOccurrencesBySlot[slot] += 1;
		}

		const restored: Manageable<any>[] = [];
		for (let slot = 0; slot <= currentUnrelated.length; slot++) {
			for (
				let occurrence = candidateOccurrencesBySlot[slot]!;
				occurrence > 0;
				occurrence--
			) {
				restored.push(program);
			}
			const child = currentUnrelated[slot];
			if (child) restored.push(child);
		}

		if (restored.length > 0) {
			const preservedReference =
				rollback.ownerChildrenPreservationTarget?.reference;
			if (preservedReference) {
				preservedReference.splice(0, preservedReference.length, ...restored);
				owner.children = preservedReference;
			} else if (currentHadOwnProperty && currentReference) {
				currentReference.splice(0, currentReference.length, ...restored);
				owner.children = currentReference;
			} else {
				owner.children = restored;
			}
		} else if (rollback.ownerChildrenPreservationTarget) {
			const target = rollback.ownerChildrenPreservationTarget;
			if (!target.hadOwnProperty) {
				delete (owner as Partial<Manageable<any>>).children;
			} else if (target.reference) {
				target.reference.splice(0, target.reference.length);
				owner.children = target.reference;
			} else {
				(owner as { children?: Manageable<any>[] }).children = undefined;
			}
		} else if (ownerPropertyChanged) {
			if (!currentHadOwnProperty) {
				delete (owner as Partial<Manageable<any>>).children;
			} else if (currentReference) {
				currentReference.splice(0, currentReference.length);
				owner.children = currentReference;
			} else {
				(owner as { children?: Manageable<any>[] }).children = undefined;
			}
		} else if (baseline.hadOwnProperty) {
			(owner as { children?: Manageable<any>[] }).children =
				baseline.value == null ? undefined : [];
		} else {
			delete (owner as Partial<Manageable<any>>).children;
		}
		this.updateRollbackOwnerChildrenWorkingSnapshot(rollback);
	}

	private restoreInitializationRollbackBaseline(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
		restoreChildren: boolean,
	): void {
		this.restoreArrayProperty(program, "parents", rollback.parents);
		if (restoreChildren) {
			this.restoreArrayProperty(program, "children", rollback.children);
		}
		this.restoreRollbackOwnerChildren(program, rollback);
	}

	private rollbackOwnerReferenceCount(
		program: Manageable<any>,
		owner: Manageable<any> | undefined,
	): number {
		return (
			program.parents?.filter((candidate) => candidate === owner).length ?? 0
		);
	}

	private rollbackInverseReferenceCount(
		program: Manageable<any>,
		owner: Manageable<any> | undefined,
	): number {
		return (
			owner?.children?.filter((candidate) => candidate === program).length ?? 0
		);
	}

	private initializationRollbackOwnerAlreadyConsumed(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): boolean {
		const baselineForward =
			rollback.parents.value?.filter(
				(candidate) => candidate === rollback.owner,
			).length ?? 0;
		const currentForward = this.rollbackOwnerReferenceCount(
			program,
			rollback.owner,
		);
		if (!rollback.owner) {
			// Root rollback has no inverse owner edge. A closed program with no
			// remaining undefined-parent reference supplies the corresponding exact
			// detachment evidence without manufacturing another private root lease.
			return terminalClosed(this, program) && currentForward === 0;
		}
		const baselineInverse =
			rollback.ownerChildren?.value?.filter(
				(candidate) => candidate === program,
			).length ?? 0;
		const currentInverse = this.rollbackInverseReferenceCount(
			program,
			rollback.owner,
		);
		return (
			(terminalClosed(this, program) &&
				currentForward === 0 &&
				currentInverse === 0) ||
			(currentForward < baselineForward && currentInverse < baselineInverse)
		);
	}

	private initializationRollbackOwnerDetached(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): boolean {
		return (
			this.rollbackOwnerReferenceCount(program, rollback.owner) === 0 &&
			(!rollback.owner ||
				this.rollbackInverseReferenceCount(program, rollback.owner) === 0)
		);
	}

	private recordInitializationRollbackOwners(
		rollback: InitializationRollbackOwner,
		owners: Iterable<Manageable<any> | undefined>,
	): void {
		for (const owner of owners) {
			if (owner) rollback.observedOwners.add(owner);
		}
	}

	private recordInitializationRollbackPostCloseState(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): void {
		this.recordInitializationRollbackOwners(rollback, program.parents ?? []);
		// The delegated base close is expected to splice the candidate from the
		// installed array, so content drift alone is not concurrent adoption here.
		// A property/reference change, however, belongs to the caller and must
		// replace any older preservation target before baseline restoration.
		this.recordRollbackOwnerChildrenIdentityDivergence(rollback);
	}

	private reconcileInitializationRollbackOwnerReferences(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): void {
		for (const owner of rollback.observedOwners) {
			const forwardReferences = this.rollbackOwnerReferenceCount(
				program,
				owner,
			);
			const inverseReferences = this.rollbackInverseReferenceCount(
				program,
				owner,
			);
			this.removeReleasedParentChildReferences(
				program,
				owner,
				Math.max(0, inverseReferences - forwardReferences),
			);
		}
	}

	private initializationRollbackGraphDetached(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): boolean {
		return (
			(program.parents?.length ?? 0) === 0 &&
			[...rollback.observedOwners].every(
				(owner) => this.rollbackInverseReferenceCount(program, owner) === 0,
			)
		);
	}

	private rollbackReleasedExactOwnerReference(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
		forwardBefore: number,
		inverseBefore: number,
	): boolean {
		const baselineForward =
			rollback.parents.value?.filter(
				(candidate) => candidate === rollback.owner,
			).length ?? 0;
		const baselineInverse = rollback.owner
			? (rollback.ownerChildren?.value?.filter(
					(candidate) => candidate === program,
				).length ?? 0)
			: 0;
		const forwardAfter = this.rollbackOwnerReferenceCount(
			program,
			rollback.owner,
		);
		const inverseAfter = this.rollbackInverseReferenceCount(
			program,
			rollback.owner,
		);
		return (
			forwardBefore === baselineForward + 1 &&
			forwardAfter === baselineForward &&
			(!rollback.owner ||
				(inverseBefore === baselineInverse + 1 &&
					inverseAfter === baselineInverse))
		);
	}

	private addCleanupResidual(
		program: Manageable<any>,
		failure: FailedTerminalCall,
		cleanupLease?: object,
	): void {
		const residual = this.reserveCleanupResidual(program, cleanupLease);
		if (
			cleanupLease &&
			residual.cleanupLease &&
			cleanupLease !== residual.cleanupLease
		) {
			releaseTerminalCleanupLease(this, program, cleanupLease);
		}
		residual.failures.push(failure);
	}

	private reserveCleanupResidual(
		program: Manageable<any>,
		cleanupLease?: object,
	): CleanupResidual {
		const existing = this._cleanupResiduals.get(program);
		if (existing) {
			return existing;
		}
		const retainedLease =
			cleanupLease ?? retainTerminalCleanupLease(this, program);
		const residual: CleanupResidual = {
			program,
			failures: [],
			cleanupLease: retainedLease,
			activeReservations: 0,
		};
		this._cleanupResiduals.set(program, residual);
		return residual;
	}

	private acquireCleanupReservation(program: Manageable<any>): CleanupResidual {
		const residual = this.reserveCleanupResidual(program);
		residual.activeReservations += 1;
		return residual;
	}

	private releaseCleanupReservation(residual: CleanupResidual): void {
		residual.activeReservations -= 1;
		if (residual.activeReservations < 0) {
			throw new Error("Cleanup residual reservation underflow");
		}
		if (
			residual.activeReservations === 0 &&
			residual.failures.length === 0 &&
			residual.terminalState == null
		) {
			this.releaseCleanupResidual(residual);
		}
	}

	private releaseCleanupResidual(residual: CleanupResidual): void {
		if (residual.activeReservations > 0) {
			throw new Error(
				"Cleanup residual cannot be released while rollback cleanup is active",
			);
		}
		if (residual.terminalState != null) {
			throw new Error(
				"Cleanup residual cannot be released while terminal identity is retained",
			);
		}
		if (residual.cleanupLease) {
			releaseTerminalCleanupLease(
				this,
				residual.program,
				residual.cleanupLease,
			);
			residual.cleanupLease = undefined;
		}
		this._cleanupResiduals.delete(residual.program);
	}

	private releaseRetainedTerminalIdentity(
		state: TerminalOperationState<T>,
	): void {
		const residual = this._cleanupResiduals.get(state.program);
		if (residual?.terminalState !== state) {
			return;
		}
		residual.terminalState = undefined;
		if (residual.activeReservations === 0 && residual.failures.length === 0) {
			this.releaseCleanupResidual(residual);
		}
	}

	private async retryCleanupResidual(
		residual: CleanupResidual,
		failure: FailedTerminalCall,
	): Promise<void> {
		const operation = (
			residual.program as Manageable<any> & {
				drop?: (...args: any[]) => Promise<boolean>;
			}
		)[failure.type];
		if (!operation) {
			throw new Error(
				`Program at ${residual.program.address} cannot retry its failed ${failure.type} cleanup`,
			);
		}
		const checkpoint = terminalCheckpoint(this, residual.program);
		const invoke = () =>
			this.invokeLifecycleMethod(() =>
				operation.apply(residual.program, failure.args),
			);
		try {
			if (failure.commit) {
				await retryCommittedTerminalCall(
					this,
					residual.program,
					failure.commit,
					invoke,
				);
			} else {
				await invoke();
			}
		} catch (error) {
			failure.commit ??= terminalCommit(
				this,
				residual.program,
				checkpoint,
				failure.type,
				failure.args[0],
			);
			throw error;
		}
	}

	private finishTerminalOperation(state: TerminalOperationState<T>): void {
		if (
			state.activeCalls > 0 ||
			state.failed ||
			!state.terminalCompleted ||
			!terminalClosed(this, state.program)
		) {
			return;
		}
		if (this.items.get(state.address) === state.program) {
			this.items.delete(state.address);
		}
		this.restoreTerminalOperations(state);
		if (
			!this._initializationRollbackOwners.has(state.program) &&
			this._failedInitializations.get(state.address) === state.program
		) {
			this._failedInitializations.delete(state.address);
		}
		if (this._terminalOperationsByAddress.get(state.address) === state) {
			this._terminalOperationsByAddress.delete(state.address);
		}
	}

	private restoreTerminalOperations(state: TerminalOperationState<T>): void {
		if (
			state.closeWrapper &&
			state.closeOperation &&
			state.program.close === state.closeWrapper
		) {
			state.program.close = state.closeOperation;
		}
		const droppable = state.program as T & {
			drop?: (...args: any[]) => Promise<boolean>;
		};
		if (
			state.dropWrapper &&
			state.dropOperation &&
			droppable.drop === state.dropWrapper
		) {
			droppable.drop = state.dropOperation;
		}
	}

	private findMonitoredPrograms(address: Address): T[] {
		const targetAddress = address.toString();
		const pending: Manageable<any>[] = [...this.items.values()];
		const visited = new Set<Manageable<any>>();
		const matches: T[] = [];
		while (pending.length > 0) {
			const candidate = pending.pop()!;
			if (visited.has(candidate)) continue;
			visited.add(candidate);
			if (candidate.address.toString() === targetAddress) {
				matches.push(candidate as T);
			}
			for (const child of candidate.children ?? []) {
				pending.push(child);
			}
		}
		return matches;
	}

	private findMonitoredProgram(
		address: Address,
		liveOnly = false,
		preferred?: Manageable<any>,
	): T | undefined {
		const matches = this.findMonitoredPrograms(address);
		if (
			preferred &&
			matches.includes(preferred as T) &&
			(!liveOnly || !terminalClosed(this, preferred))
		) {
			return preferred as T;
		}
		return liveOnly
			? matches.find((candidate) => !terminalClosed(this, candidate))
			: (matches.find((candidate) => !terminalClosed(this, candidate)) ??
					matches[0]);
	}

	private attachExistingProgram<S extends T>(
		address: Address,
		program: S,
		parent?: Manageable<any>,
	): S {
		if (parent) this.assertParentCanOwnOpen(parent);
		addParent(program, parent);
		if (parent == null && this.items.get(address) !== program) {
			const direct = this.items.get(address);
			if (direct && !terminalClosed(this, direct) && direct !== program) {
				throw new Error(
					`Program at ${address} is already monitored as another live instance`,
				);
			}
			this.monitorTerminalOperations(address, program);
			this.items.set(address, program);
		}
		return program;
	}

	private assertNoTerminalOperation(
		address: Address,
		requestedProgram?: Manageable<any>,
		allowSettledAncestorRepair = requestedProgram != null,
	): void {
		const rollbackOwner = [
			...this._initializationRollbackOwners.entries(),
		].find(
			([program, rollback]) =>
				program.address.toString() === address.toString() ||
				rollback.address.toString() === address.toString(),
		);
		if (rollbackOwner) {
			throw new Error(
				`Program at ${address} failed initialization cleanup and has pending rollback cleanup that must be retried before reopen`,
			);
		}
		const residual = [...this._cleanupResiduals.values()].find(
			(candidate) =>
				candidate.program.address.toString() === address.toString(),
		);
		if (residual) {
			throw new Error(
				`Program at ${address} has pending cleanup residuals and cannot be reopened before cleanup is retried`,
			);
		}
		const state = this._terminalOperationsByAddress.get(address);
		if (state?.activeCalls) {
			throw new Error(
				`Program is terminating and cannot accept another parent while its close or drop operation is finishing (${address})`,
			);
		}
		if (state?.failed) {
			throw new Error(
				`Program at ${address} failed terminal cleanup and cannot be reopened before cleanup is retried`,
			);
		}
		const targetAddress = address.toString();
		const pending: {
			program: Manageable<any>;
			ancestorActive: boolean;
			ancestorSettled: boolean;
			ancestorResidual: boolean;
		}[] = [...this.items.values()].map((program) => ({
			program,
			ancestorActive: false,
			ancestorSettled: false,
			ancestorResidual: false,
		}));
		for (const cleanupResidual of this._cleanupResiduals.values()) {
			pending.push({
				program: cleanupResidual.program,
				ancestorActive: false,
				ancestorSettled: false,
				ancestorResidual: true,
			});
		}
		const visited = new Map<Manageable<any>, number>();
		while (pending.length > 0) {
			const {
				program: candidate,
				ancestorActive,
				ancestorSettled,
				ancestorResidual,
			} = pending.pop()!;
			const candidateState = this._terminalOperationsByProgram.get(
				candidate as T,
			);
			const ownActive = (candidateState?.activeCalls ?? 0) > 0;
			const ownFailed = candidateState?.failed === true;
			const ownAttachmentFence =
				candidate.acceptsParentAttachments === false &&
				(!terminalClosed(this, candidate) ||
					candidate.pendingTerminalOperation != null ||
					ownActive ||
					ownFailed);
			const stateMask =
				(ancestorActive ? 1 : 0) |
				(ancestorSettled ? 2 : 0) |
				(ancestorResidual ? 4 : 0);
			const previousMask = visited.get(candidate) ?? -1;
			if (previousMask !== -1 && (previousMask | stateMask) === previousMask) {
				continue;
			}
			visited.set(
				candidate,
				previousMask === -1 ? stateMask : previousMask | stateMask,
			);
			if (candidate.address.toString() === targetAddress) {
				const exactRepair =
					allowSettledAncestorRepair && requestedProgram === candidate;
				if (
					ownActive ||
					ownFailed ||
					ownAttachmentFence ||
					ancestorActive ||
					ancestorResidual ||
					(ancestorSettled && !exactRepair)
				) {
					throw new Error(
						`Program is terminating and cannot accept another parent while its cleanup is finishing (${address})`,
					);
				}
			}
			const descendantActive = ancestorActive || ownActive;
			const descendantSettled =
				ancestorSettled || (!ownActive && (ownFailed || ownAttachmentFence));
			for (const child of candidate.children ?? []) {
				pending.push({
					program: child,
					ancestorActive: descendantActive,
					ancestorSettled: descendantSettled,
					ancestorResidual,
				});
			}
		}
	}

	private assertTerminalGraphCanStart(program: Manageable<any>): void {
		if (this._initializationRollbacks.has(program)) {
			return;
		}
		const replacing = this._replacementClosures.has(program);
		const pending = [program];
		const visited = new Set<Manageable<any>>();
		while (pending.length > 0) {
			const candidate = pending.pop()!;
			if (visited.has(candidate)) continue;
			visited.add(candidate);
			if ((this._parentAttachmentReservations.get(candidate) ?? 0) > 0) {
				throw new TerminalOperationNotStartedError(
					`Program at ${candidate.address} has an opening child attachment that must finish first`,
				);
			}
			const address = candidate.address.toString();
			const rootReplacementOpen = replacing && candidate === program;
			if (
				(this._openingPromises.has(address) && !rootReplacementOpen) ||
				(this._openingReservationsByAddress.get(address)?.size ?? 0) > 0
			) {
				throw new TerminalOperationNotStartedError(
					`Program at ${address} cannot terminate while an open generation owns its address`,
				);
			}
			for (const child of candidate.children ?? []) {
				pending.push(child);
			}
		}
	}

	private async waitForTerminalOperations(
		program: Manageable<any>,
	): Promise<void> {
		const state = this._terminalOperationsByProgram.get(program);
		while (state && state.pending.size > 0) {
			// A rejected direct close is not itself the result of stop(). Once it has
			// settled, closeCompletely() retries the retained instance below.
			await Promise.allSettled([...state.pending]);
		}
	}

	private installInitializationRollbackOwner(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): { forward: number; inverse: number } {
		// Compare with the state left by the previous Handler restoration before
		// that restoration runs again. A caller may have adopted the property while
		// rollback was quarantined; observing here avoids confusing the later base
		// close splice with a concurrent mutation.
		this.recordRollbackOwnerChildrenDivergence(rollback);
		this.restoreInitializationRollbackBaseline(program, rollback, false);
		const parents = [...(rollback.parents.value ?? []), rollback.owner];
		program.parents = parents;
		if (rollback.owner) {
			if (rollback.owner.children) {
				rollback.owner.children.push(program);
			} else {
				rollback.owner.children = [program];
			}
			this.updateRollbackOwnerChildrenWorkingSnapshot(rollback);
		}
		return {
			forward: this.rollbackOwnerReferenceCount(program, rollback.owner),
			inverse: this.rollbackInverseReferenceCount(program, rollback.owner),
		};
	}

	private async retryPendingInitializationRollbackOwner(
		program: Manageable<any>,
		rollback: InitializationRollbackOwner,
	): Promise<void> {
		if (rollback.phase !== "pending-pre-base") return;
		if (this.initializationRollbackOwnerAlreadyConsumed(program, rollback)) {
			// A previous stop attempt can fail the private retry and then close the
			// monitored ancestor, which spends the restored baseline edge while the
			// rollback record remains pending. That terminal/detached graph is already
			// the successful cleanup result; reinstalling its baseline plus private lease
			// would resurrect a closed child or an owner edge that was consumed once.
			rollback.phase = "consumed";
			return;
		}

		// Re-wrap replacements before borrowing the private owner edge. The retry must
		// pass through the same Handler terminal monitor as ordinary close calls.
		this.monitorTerminalOperations(rollback.address, program as T);
		const checkpoint = terminalCheckpoint(this, program);
		const installed = this.installInitializationRollbackOwner(
			program,
			rollback,
		);
		let invocationSettled = false;
		const childCleanupErrors: unknown[] = [];
		try {
			const closed = await (
				program.close as unknown as (from?: Manageable<any>) => Promise<boolean>
			).call(program, rollback.owner);
			invocationSettled = true;
			const releasedExactOwner = this.rollbackReleasedExactOwnerReference(
				program,
				rollback,
				installed.forward,
				installed.inverse,
			);
			if (!releasedExactOwner || (closed && !terminalClosed(this, program))) {
				throw new Error(
					`Program at ${rollback.address} did not release its initialization rollback owner`,
				);
			}
			rollback.phase = "consumed";
		} catch (error) {
			if (
				!invocationSettled &&
				terminalCommit(this, program, checkpoint, "close", rollback.owner)
			) {
				// Base close already owns this generation. Its public retry contract
				// consumes the recorded commit and must not see a manufactured owner.
				rollback.phase = "committed";
			}
			throw error;
		} finally {
			// A replacement close can attach a parent while releasing the private
			// rollback lease. Capture every such owner before restoring the saved
			// forward edge shape, otherwise its inverse children edge can become
			// unreachable to the final graph reconciliation.
			this.recordInitializationRollbackPostCloseState(program, rollback);
			this.restoreInitializationRollbackBaseline(program, rollback, false);
			await this.cleanupChildReferenceExcesses(
				rollback.address,
				program,
				rollback.children,
				rollback.childCandidates,
				childCleanupErrors,
			);
			if (childCleanupErrors.length > 0) {
				logger.error(
					`Initialization rollback retry at ${rollback.address} encountered ${childCleanupErrors.length} child cleanup error(s)`,
				);
			}
		}
	}

	private observeTerminalGraph(
		program: Manageable<any>,
		childCandidates: Set<Manageable<any>>,
		owners: Set<Manageable<any>>,
	): void {
		for (const child of program.children ?? []) {
			if (child !== program) childCandidates.add(child);
		}
		for (const parent of program.parents ?? []) {
			if (parent) owners.add(parent);
		}
		try {
			for (const dependency of this.properties.getDependencies?.(
				program as T,
			) ?? []) {
				if (dependency !== program) childCandidates.add(dependency);
			}
		} catch (error) {
			// A terminal object may make application dependency getters unavailable.
			// Current graph edges remain authoritative; do not turn observation into a
			// second, unrelated terminal failure.
			logger.trace(
				`Could not enumerate terminal dependencies for ${program.address}: ${String(error)}`,
			);
		}
	}

	private reconcileClosedProgramParents(
		program: Manageable<any>,
		owners: Set<Manageable<any>>,
	): void {
		for (const parent of program.parents ?? []) {
			if (parent) owners.add(parent);
		}
		program.parents?.splice(0, program.parents.length);
		for (const owner of owners) {
			let childIndex = owner.children?.indexOf(program) ?? -1;
			while (childIndex !== -1) {
				owner.children.splice(childIndex, 1);
				childIndex = owner.children.indexOf(program);
			}
		}
	}

	private async closeCompletely(program: Manageable<any>): Promise<void> {
		const terminalChildCandidates = new Set<Manageable<any>>();
		const terminalOwners = new Set<Manageable<any>>();
		this.observeTerminalGraph(program, terminalChildCandidates, terminalOwners);
		await this.waitForTerminalOperations(program);
		this.observeTerminalGraph(program, terminalChildCandidates, terminalOwners);
		let rollbackOwner = this._initializationRollbackOwners.get(program);
		if (rollbackOwner) {
			this.recordInitializationRollbackOwners(
				rollbackOwner,
				program.parents ?? [],
			);
		}
		if (rollbackOwner?.phase === "pending-pre-base") {
			await this.retryPendingInitializationRollbackOwner(
				program,
				rollbackOwner,
			);
			await this.waitForTerminalOperations(program);
			this.observeTerminalGraph(
				program,
				terminalChildCandidates,
				terminalOwners,
			);
		}
		let terminalState = this._terminalOperationsByProgram.get(program);
		while (!terminalClosed(this, program) || terminalState?.failed) {
			if (terminalState) {
				// Applications/tests may replace a failed wrapped method before stop.
				// Re-wrap any replacement in the existing identity-specific state before
				// invoking it, so both a fresh failure and exact recovery remain monitored.
				this.refreshTerminalOperationWrappers(terminalState);
			}
			const wasRecovering = terminalState?.failed === true;
			const failedCall = terminalState?.failedCall;
			const ownersBefore = [...(program.parents ?? [])];
			this.observeTerminalGraph(
				program,
				terminalChildCandidates,
				terminalOwners,
			);
			if (rollbackOwner) {
				this.recordInitializationRollbackOwners(rollbackOwner, ownersBefore);
			}
			const owner = wasRecovering
				? (failedCall?.args[0] as Manageable<any> | undefined)
				: terminalClosed(this, program)
					? undefined
					: ownersBefore[0];
			const ownerReferencesBefore = ownersBefore.filter(
				(candidate) => candidate === owner,
			).length;
			const operationType: TerminalOperation =
				failedCall?.type ??
				(terminalState?.failedOperation === "drop" ? "drop" : "close");
			const operation = (
				program as Manageable<any> & {
					drop?: (from?: Manageable<any>) => Promise<boolean>;
				}
			)[operationType];
			if (!operation) {
				throw new Error(
					`Program at ${program.address} cannot retry its failed ${operationType} cleanup`,
				);
			}
			const operationArgs = wasRecovering ? (failedCall?.args ?? []) : [owner];
			const closed = await (
				operation as (...args: any[]) => Promise<boolean>
			).apply(program, operationArgs);
			await this.waitForTerminalOperations(program);
			this.observeTerminalGraph(
				program,
				terminalChildCandidates,
				terminalOwners,
			);
			terminalState = this._terminalOperationsByProgram.get(program);
			rollbackOwner = this._initializationRollbackOwners.get(program);
			if (rollbackOwner?.phase === "committed" && !terminalState?.failed) {
				rollbackOwner.phase = "consumed";
			}
			if (
				terminalState?.failed &&
				terminalState.failedCall === failedCall &&
				terminalState.failedOperation === operationType &&
				((closed && terminalClosed(this, program)) ||
					(failedCall?.commit != null && closed === failedCall.commit.result))
			) {
				// This also supports a test/application replacing the wrapped method
				// after open: closeCompletely awaited the full replacement call, so it
				// is safe to mark the previously retained cleanup as recovered. A
				// committed non-terminal release must reproduce its recorded `false`;
				// the fresh loop below then drains the owners that remain.
				terminalState.failed = false;
				terminalState.failedOperation = undefined;
				terminalState.failedCall = undefined;
				terminalState.terminalCompleted =
					closed && terminalClosed(this, program);
				this.releaseRetainedTerminalIdentity(terminalState);
				if (terminalState.cleanupLease) {
					releaseTerminalCleanupLease(
						this,
						program,
						terminalState.cleanupLease,
					);
					terminalState.cleanupLease = undefined;
				}
				this.finishTerminalOperation(terminalState);
			}

			const ownersAfter = program.parents ?? [];
			const ownerReferencesAfter = ownersAfter.filter(
				(candidate) => candidate === owner,
			).length;
			if (owner) {
				const inverseEdges =
					owner.children?.filter((candidate) => candidate === program).length ??
					0;
				this.removeReleasedParentChildReferences(
					program,
					owner,
					Math.max(0, inverseEdges - ownerReferencesAfter),
				);
			}
			if (rollbackOwner) {
				this.recordInitializationRollbackOwners(rollbackOwner, ownersAfter);
				this.reconcileInitializationRollbackOwnerReferences(
					program,
					rollbackOwner,
				);
			}
			if (terminalClosed(this, program) && !terminalState?.failed) {
				break;
			}
			if (wasRecovering && !terminalState?.failed) {
				// The failed outer call has now completed. Its base ownership transition
				// may already have committed, so evaluate remaining owners on a fresh loop.
				continue;
			}
			// A non-terminal Program.close(from) is valid only when it releases the
			// requested owner and strictly reduces the total owner count. The latter is
			// a monotonic measure that bounds this loop.
			if (
				closed ||
				ownersAfter.length >= ownersBefore.length ||
				ownerReferencesAfter >= ownerReferencesBefore
			) {
				throw new Error(
					`Program at ${program.address} did not make ownership progress while stopping (${ownersBefore.length} -> ${ownersAfter.length})`,
				);
			}
		}
		this.observeTerminalGraph(program, terminalChildCandidates, terminalOwners);
		if (terminalClosed(this, program)) {
			const childCleanupErrors: unknown[] = [];
			await this.cleanupChildReferenceExcesses(
				program.address,
				program,
				{ hadOwnProperty: true, value: [] },
				terminalChildCandidates,
				childCleanupErrors,
				(child) => {
					const childRollback = this._initializationRollbackOwners.get(child);
					return childRollback != null && childRollback.phase !== "consumed";
				},
			);
			this.observeTerminalGraph(
				program,
				terminalChildCandidates,
				terminalOwners,
			);
			this.reconcileClosedProgramParents(program, terminalOwners);
			if (childCleanupErrors.length > 0) {
				throw childCleanupErrors[0];
			}
		}
		rollbackOwner = this._initializationRollbackOwners.get(program);
		if (rollbackOwner) {
			this.recordInitializationRollbackOwners(
				rollbackOwner,
				program.parents ?? [],
			);
			if (terminalClosed(this, program) && !terminalState?.failed) {
				this.reconcileInitializationRollbackOwnerReferences(
					program,
					rollbackOwner,
				);
			}
			if (
				!terminalClosed(this, program) ||
				terminalState?.failed ||
				rollbackOwner.phase !== "consumed" ||
				!this.initializationRollbackGraphDetached(program, rollbackOwner)
			) {
				throw new Error(
					`Program at ${rollbackOwner.address} still has initialization rollback cleanup`,
				);
			}
			this._initializationRollbackOwners.delete(program);
			if (this._failedInitializations.get(rollbackOwner.address) === program) {
				this._failedInitializations.delete(rollbackOwner.address);
			}
		}
	}

	private removeReleasedParentChildReferences(
		program: Manageable<any>,
		parent: Manageable<any> | undefined,
		releasedReferences: number,
	) {
		if (!parent) {
			return;
		}
		while (releasedReferences > 0) {
			const childIndex = parent.children?.indexOf(program) ?? -1;
			if (childIndex === -1) {
				return;
			}
			parent.children.splice(childIndex, 1);
			releasedReferences -= 1;
		}
	}

	private restoreUncommittedParentChildReferences(
		program: Manageable<any>,
		startedReferences: Map<Manageable<any>, number>,
		observedParents: (Manageable<any> | undefined)[],
	): void {
		const affectedParents = new Set<Manageable<any>>(startedReferences.keys());
		for (const parent of observedParents) {
			if (parent != null) affectedParents.add(parent);
		}
		for (const parent of affectedParents) {
			const expectedReferences = startedReferences.get(parent) ?? 0;
			let currentReferences =
				parent.children?.filter((child) => child === program).length ?? 0;
			while (currentReferences > expectedReferences) {
				const childIndex = parent.children?.lastIndexOf(program) ?? -1;
				if (childIndex === -1) break;
				parent.children!.splice(childIndex, 1);
				currentReferences -= 1;
			}
			while (currentReferences < expectedReferences) {
				(parent.children || (parent.children = [])).push(program);
				currentReferences += 1;
			}
		}
	}

	private restoreUncommittedParentReferences(
		program: Manageable<any>,
		baseline: ArrayPropertyState<Manageable<any> | undefined>,
	): void {
		if (!baseline.hadOwnProperty) {
			delete (program as Partial<Manageable<any>>).parents;
			return;
		}
		if (baseline.reference) {
			baseline.reference.splice(
				0,
				baseline.reference.length,
				...(baseline.value ?? []),
			);
			program.parents = baseline.reference;
			return;
		}
		program.parents = undefined;
	}

	private assertNoFailedInitialization(address: Address) {
		if (
			[...this._initializationRollbackOwners.values()].some(
				(rollback) => rollback.address.toString() === address.toString(),
			)
		) {
			throw new Error(
				`Program at ${address} failed initialization cleanup and has pending rollback cleanup that must be stopped before reopen`,
			);
		}
		const failed = this._failedInitializations.get(address);
		if (!failed) {
			return;
		}
		if (terminalClosed(this, failed)) {
			if (this._terminalOperationsByAddress.get(address)?.failed) {
				return;
			}
			this._failedInitializations.delete(address);
			if (this.items.get(address) === failed) {
				this.items.delete(address);
			}
			return;
		}
		throw new Error(
			`Program at ${address} failed initialization cleanup and cannot be reopened before it is stopped`,
		);
	}

	private async stopOnce(): Promise<void> {
		// Do not PQueue.clear(): p-queue drops queued callbacks without settling the
		// promises returned to callers. Admitted opens must run to completion (or
		// rejection) before their programs can be closed below.
		await Promise.all([...this._openAdmissions]);
		await Promise.all(
			[...this._openQueue.values()].map((queue) => queue.onIdle()),
		);
		this._openQueue.clear();

		// Wait for any in-progress opens to complete before closing
		// This prevents race conditions where a program is being opened while we close
		if (this._openingPromises.size > 0) {
			await Promise.allSettled([...this._openingPromises.values()]);
		}
		this._openingPromises.clear();
		this._openingReservations.clear();
		this._openingReservationsByAddress.clear();

		// A failed child can have its wrapped terminal method replaced before stop.
		// Refresh every retained failed identity before closing any parent root, so
		// parent traversal consumes the recorded commit through the new wrapper
		// instead of calling the replacement outside Handler monitoring.
		const pendingTerminalGraphs: Manageable<any>[] = [
			...this._initializationRollbackOwners.keys(),
			...this.items.values(),
			...this._cleanupResiduals.keys(),
		];
		const visitedTerminalGraphs = new Set<Manageable<any>>();
		while (pendingTerminalGraphs.length > 0) {
			const candidate = pendingTerminalGraphs.pop()!;
			if (visitedTerminalGraphs.has(candidate)) continue;
			visitedTerminalGraphs.add(candidate);
			const terminalState = this._terminalOperationsByProgram.get(candidate);
			if (terminalState?.failed) {
				this.refreshTerminalOperationWrappers(terminalState);
			}
			for (const child of candidate.children ?? []) {
				pendingTerminalGraphs.push(child);
			}
		}

		// A close can legitimately return false when it only releases one of several
		// owners. Drain every ownership reference so stop cannot discard a still-live
		// program from Handler state.
		const closeErrors: unknown[] = [];
		const cleanupTargets: Array<[Address, T]> = [];
		const targetedPrograms = new Set<Manageable<any>>();
		// Private rollback owners go first. Their public graph was intentionally
		// restored, so they must not depend on an ancestor still exposing an edge.
		for (const [program, rollback] of this._initializationRollbackOwners) {
			cleanupTargets.push([rollback.address, program as T]);
			targetedPrograms.add(program);
		}
		for (const [address, program] of this.items) {
			if (targetedPrograms.has(program)) continue;
			cleanupTargets.push([address, program]);
			targetedPrograms.add(program);
		}
		for (const [address, program] of cleanupTargets) {
			try {
				await this.closeCompletely(program);
			} catch (error) {
				closeErrors.push(error);
			}
			const terminalState = this._terminalOperationsByProgram.get(program);
			if (
				terminalClosed(this, program) &&
				!terminalState?.failed &&
				(terminalState?.activeCalls ?? 0) === 0 &&
				this.items.get(address) === program
			) {
				this.items.delete(address);
			}
			if (
				terminalClosed(this, program) &&
				!terminalState?.failed &&
				!this._initializationRollbackOwners.has(program) &&
				this._failedInitializations.get(address) === program
			) {
				this._failedInitializations.delete(address);
			}
		}
		for (const [program, residual] of [...this._cleanupResiduals]) {
			try {
				for (const failure of [...residual.failures]) {
					await this.retryCleanupResidual(residual, failure);
					const failureIndex = residual.failures.indexOf(failure);
					if (failureIndex !== -1) residual.failures.splice(failureIndex, 1);
				}
				await this.closeCompletely(program);
			} catch (error) {
				closeErrors.push(error);
			}
			const terminalState = this._terminalOperationsByProgram.get(program);
			if (
				terminalClosed(this, program) &&
				residual.failures.length === 0 &&
				residual.terminalState == null &&
				!terminalState?.failed &&
				(terminalState?.activeCalls ?? 0) === 0
			) {
				this.releaseCleanupResidual(residual);
			}
		}
		if (closeErrors.length > 0) {
			throw closeErrors[0];
		}
		if (this._initializationRollbackOwners.size > 0) {
			throw new Error(
				"Program handler still has initialization rollback owners after cleanup",
			);
		}

		this.items = new Map();
		this._failedInitializations.clear();
		for (const residual of [...this._cleanupResiduals.values()]) {
			this.releaseCleanupResidual(residual);
		}
		for (const state of new Set(this._terminalOperationsByAddress.values())) {
			this.restoreTerminalOperations(state);
		}
		this._terminalOperationsByAddress.clear();
	}

	private _onProgamClose(program: Manageable<any>) {
		const address = program.address!.toString();
		const terminalState = this._terminalOperationsByProgram.get(program);
		if (terminalState?.activeCalls || terminalState?.failed) {
			// The base Program has ended, but the dynamically-dispatched subclass
			// close/drop promise is still running. Its wrapper owns final eviction.
			return;
		}
		if (this.items.get(address) === program) {
			this.items.delete(address);
		}
		if (
			!this._initializationRollbackOwners.has(program) &&
			this._failedInitializations.get(address) === program
		) {
			this._failedInitializations.delete(address);
		}
		// TODO remove item from this._openQueue?
	}

	private async closeForReplacement(
		address: Address,
		program: Manageable<any>,
		parent?: Manageable<any>,
	): Promise<void> {
		const owners = program.parents ?? [];
		if (owners.length > 1 || (owners.length === 1 && owners[0] !== parent)) {
			throw new Error(
				`Program at ${address} cannot be replaced while it has other owners`,
			);
		}
		const parentReferencesBefore = owners.filter(
			(candidate) => candidate === parent,
		).length;
		let closed: boolean;
		this._replacementClosures.add(program);
		try {
			closed = await (
				program.close as unknown as (from?: Manageable<any>) => Promise<boolean>
			).call(program, parent);
		} finally {
			this._replacementClosures.delete(program);
		}
		const parentReferencesAfter = (program.parents ?? []).filter(
			(candidate) => candidate === parent,
		).length;
		this.removeReleasedParentChildReferences(
			program,
			parent,
			parentReferencesBefore - parentReferencesAfter,
		);
		if (
			!closed ||
			!terminalClosed(this, program) ||
			parentReferencesAfter > 0
		) {
			throw new Error(
				`Program at ${address} cannot be replaced because close was not terminal`,
			);
		}
		if (this.items.get(address) === program) {
			this.items.delete(address);
		}
	}

	private async checkProcessExisting<S extends T>(
		address: Address,
		toOpen: Manageable<any>,
		mergeSrategy: ProgramMergeStrategy = "reject",
		parent?: Manageable<any>,
	): Promise<S | undefined> {
		this.assertNoFailedInitialization(address);
		this.assertNoTerminalOperation(address, toOpen);
		const prev = this.findMonitoredProgram(address, false, toOpen);
		if (prev && terminalClosed(this, prev)) {
			if (this.items.get(address) === prev) {
				this.items.delete(address);
			}
			return undefined;
		}
		if (mergeSrategy === "reject") {
			if (prev) {
				throw new Error(`Program at ${address} is already open`);
			}
		} else if (mergeSrategy === "replace") {
			if (prev && prev !== toOpen) {
				await this.closeForReplacement(address, prev, parent);
			}
		} else if (mergeSrategy === "reuse") {
			if (prev) {
				this.attachExistingProgram(address, prev, parent);
			}
			return prev as S;
		}
	}

	private async processParentExisting<S extends T>(
		address: Address,
		requestedProgram: Manageable<any> | undefined,
		mergeStrategy: ProgramMergeStrategy,
		parent: Manageable<any>,
	): Promise<S | undefined> {
		this.assertNoFailedInitialization(address);
		this.assertNoTerminalOperation(address, requestedProgram);
		const existing = this.findMonitoredProgram(
			address,
			false,
			requestedProgram,
		);
		if (existing && terminalClosed(this, existing)) {
			if (this.items.get(address) === existing) {
				this.items.delete(address);
			}
			return undefined;
		}
		if (!existing) {
			return undefined;
		}

		if (mergeStrategy === "reject") {
			throw new Error(`Program at ${address} is already open`);
		}
		if (mergeStrategy === "replace" && existing !== requestedProgram) {
			await this.closeForReplacement(address, existing, parent);
			return undefined;
		}

		this.attachExistingProgram(address, existing, parent);
		try {
			// A program explicitly opened with a parent is a weak/addressable
			// reference. Structural beforeOpen() may already have made the exact
			// instance live without persisting its standalone block.
			const blocks = this.properties.client.services.blocks;
			if (!(await blocks.has(address))) {
				await existing.save(blocks, { skipOnAddress: false });
			}
		} catch (error) {
			const parentIndex = existing.parents?.lastIndexOf(parent) ?? -1;
			if (parentIndex !== -1) existing.parents.splice(parentIndex, 1);
			const childIndex = parent.children?.lastIndexOf(existing) ?? -1;
			if (childIndex !== -1) parent.children.splice(childIndex, 1);
			throw error;
		}
		return existing as S;
	}

	private async cleanupInitializationRollbackChildOccurrence(
		address: Address,
		program: Manageable<any>,
		child: Manageable<any>,
		cleanupErrors: unknown[],
	): Promise<void> {
		const referencesBefore =
			child.parents?.filter((parent) => parent === program).length ?? 0;
		let retainedFailure = false;
		let cleanupReservation: CleanupResidual | undefined;
		if (!terminalClosed(this, child) && referencesBefore > 0) {
			const checkpoint = terminalCheckpoint(this, child);
			cleanupReservation = this.acquireCleanupReservation(child);
			try {
				await (
					child.close as unknown as (from?: Manageable<any>) => Promise<boolean>
				).call(child, program);
			} catch (cleanupError) {
				retainedFailure = true;
				const terminalState = this._terminalOperationsByProgram.get(child as T);
				const handlerOwnsFailure =
					terminalState?.failed === true &&
					terminalState.failedCall?.type === "close" &&
					this.sameTerminalArgs(terminalState.failedCall.args, [program]);
				if (!handlerOwnsFailure) {
					cleanupReservation.failures.push({
						type: "close",
						args: [program],
						commit: terminalCommit(this, child, checkpoint, "close", program),
					});
				}
				this.releaseCleanupReservation(cleanupReservation);
				cleanupReservation = undefined;
				cleanupErrors.push(cleanupError);
				logger.error(
					`Failed to close partially opened child of ${address}: ${String(
						cleanupError,
					)}`,
				);
			}
		}
		let referencesAfter =
			child.parents?.filter((parent) => parent === program).length ?? 0;
		if (
			!retainedFailure &&
			referencesAfter >= referencesBefore &&
			referencesAfter > 0
		) {
			const parentIndex = child.parents.lastIndexOf(program);
			child.parents.splice(parentIndex, 1);
			referencesAfter -= 1;
		}
		if (cleanupReservation) {
			this.releaseCleanupReservation(cleanupReservation);
		}
		const remainingProgramReferences =
			child.parents?.filter((parent) => parent === program).length ?? 0;
		if (!retainedFailure) {
			const inverseReferences =
				program.children?.filter((candidate) => candidate === child).length ??
				0;
			if (inverseReferences > remainingProgramReferences) {
				const childIndex = program.children.indexOf(child);
				if (childIndex !== -1) program.children.splice(childIndex, 1);
			}
		}
	}

	private rollbackChildReferenceExcess(
		program: Manageable<any>,
		child: Manageable<any>,
		baselineCount: number,
	): number {
		const inverseReferences =
			program.children?.filter((candidate) => candidate === child).length ?? 0;
		const forwardReferences =
			child.parents?.filter((parent) => parent === program).length ?? 0;
		return Math.max(
			0,
			Math.max(inverseReferences, forwardReferences) - baselineCount,
		);
	}

	private async cleanupChildReferenceExcesses(
		address: Address,
		program: Manageable<any>,
		baseline: ArrayPropertyBaseline<Manageable<any>>,
		childCandidates: Set<Manageable<any>>,
		cleanupErrors: unknown[],
		preserveChild?: (child: Manageable<any>) => boolean,
	): Promise<void> {
		const baselineChildCounts = new Map<Manageable<any>, number>();
		for (const child of baseline.value ?? []) {
			baselineChildCounts.set(child, (baselineChildCounts.get(child) ?? 0) + 1);
			childCandidates.add(child);
		}

		// A blocked count records evidence on which one cleanup attempt made no
		// progress. New evidence above that high-water mark is still processed, while
		// an adversarial false/no-op close cannot make this scan loop forever.
		const blockedAt = new Map<Manageable<any>, number>();
		while (true) {
			for (const child of program.children ?? []) {
				childCandidates.add(child);
			}
			let next:
				| { child: Manageable<any>; baselineCount: number; excess: number }
				| undefined;
			for (const child of childCandidates) {
				if (child === program || preserveChild?.(child)) continue;
				const baselineCount = baselineChildCounts.get(child) ?? 0;
				const excess = this.rollbackChildReferenceExcess(
					program,
					child,
					baselineCount,
				);
				const blockedCount = blockedAt.get(child);
				if (excess > 0 && (blockedCount == null || excess > blockedCount)) {
					next = { child, baselineCount, excess };
					break;
				}
			}
			if (!next) break;
			await this.cleanupInitializationRollbackChildOccurrence(
				address,
				program,
				next.child,
				cleanupErrors,
			);
			const excessAfter = this.rollbackChildReferenceExcess(
				program,
				next.child,
				next.baselineCount,
			);
			if (excessAfter >= next.excess) {
				blockedAt.set(next.child, excessAfter);
			} else {
				blockedAt.delete(next.child);
			}
		}

		// A failed generation can erase both sides of a newly opened child edge, and
		// a hostile extra-occurrence close can over-release a legitimate baseline.
		// Retain either remaining excess ownership or a still-live ownerless program;
		// deciding after the full scan avoids retaining an already-drained duplicate.
		for (const child of childCandidates) {
			if (child === program) continue;
			const baselineCount = baselineChildCounts.get(child) ?? 0;
			const excess = this.rollbackChildReferenceExcess(
				program,
				child,
				baselineCount,
			);
			if (preserveChild?.(child)) {
				if (excess > 0) {
					cleanupErrors.push(
						new Error(
							`Child program at ${child.address} still has initialization rollback cleanup`,
						),
					);
				}
				continue;
			}
			if (terminalClosed(this, child) || this._cleanupResiduals.has(child)) {
				continue;
			}
			if (
				this.items.get(child.address.toString()) === (child as T) ||
				this._terminalOperationsByProgram.get(child as T)?.failed === true
			) {
				// The normal managed identity/terminal retry path already owns this
				// program. A second residual would replay the same owner release after the
				// managed close has drained it.
				continue;
			}
			if (excess > 0) {
				this.addCleanupResidual(child, { type: "close", args: [program] });
			} else if ((child.parents?.length ?? 0) === 0) {
				this.addCleanupResidual(child, { type: "close", args: [undefined] });
			}
		}
	}

	private restoreInitializationRollbackChildren(
		program: Manageable<any>,
		baseline: ArrayPropertyBaseline<Manageable<any>>,
	): void {
		const remainingByChild = new Map<Manageable<any>, number>();
		const restored: Manageable<any>[] = [];
		for (const child of baseline.value ?? []) {
			let remaining = remainingByChild.get(child);
			if (remaining == null) {
				remaining = terminalClosed(this, child)
					? 0
					: (child.parents?.filter((parent) => parent === program).length ?? 0);
			}
			if (remaining > 0) {
				restored.push(child);
				remaining -= 1;
			}
			remainingByChild.set(child, remaining);
		}
		if (!baseline.hadOwnProperty) {
			if (restored.length === 0) {
				delete (program as Partial<Manageable<any>>).children;
			} else {
				program.children = restored;
			}
			return;
		}
		(program as { children?: Manageable<any>[] }).children =
			baseline.value == null ? undefined : restored;
	}

	private async rollbackFailedInitialization(
		address: Address,
		program: Manageable<any>,
		state: InitializationRollbackBaseline,
		knownPrograms: Iterable<Manageable<any>>,
	): Promise<void> {
		if (this.items.get(address) === program) {
			this.items.delete(address);
		}

		const cleanupErrors: unknown[] = [];
		const childrenAfterFailure = [...(program.children ?? [])];
		const rollbackOwner: InitializationRollbackOwner = {
			address,
			owner: state.parent,
			phase: "pending-pre-base",
			parents: state.parents,
			children: state.children,
			ownerChildren: state.parentChildren,
			ownerChildrenWorkingReference: state.parentChildrenWorkingReference,
			ownerChildrenWorkingSnapshot: state.parentChildrenWorkingSnapshot,
			observedOwners: new Set(
				(state.parents.value ?? []).filter(
					(owner): owner is Manageable<any> => owner != null,
				),
			),
			childCandidates: new Set(
				[
					...(state.children.value ?? []),
					...childrenAfterFailure,
					...knownPrograms,
				].filter((candidate) => candidate !== program),
			),
		};
		if (state.parent) rollbackOwner.observedOwners.add(state.parent);
		let retainRollbackOwner = false;
		// Snapshot comparison must happen before close(). A successful base close is
		// expected to splice its exact owner edge from this same array, whereas a
		// caller mutation that already changed the array is concurrent state that the
		// rollback must preserve.
		this.adoptExactGeneratedRollbackOwnerChildrenReference(rollbackOwner);
		this.recordRollbackOwnerChildrenDivergence(rollbackOwner);
		if (!terminalClosed(this, program)) {
			const checkpoint = terminalCheckpoint(this, program);
			const forwardBefore = this.rollbackOwnerReferenceCount(
				program,
				rollbackOwner.owner,
			);
			const inverseBefore = this.rollbackInverseReferenceCount(
				program,
				rollbackOwner.owner,
			);
			try {
				// Manageable's historical Closeable surface omits the ownership argument.
				// Keep the widening local to this exact Handler-controlled call.
				const closed = await (
					program.close as unknown as (
						from?: Manageable<any>,
					) => Promise<boolean>
				).call(program, state.parent);
				const releasedExactOwner = this.rollbackReleasedExactOwnerReference(
					program,
					rollbackOwner,
					forwardBefore,
					inverseBefore,
				);
				if (
					closed &&
					terminalClosed(this, program) &&
					releasedExactOwner &&
					this.initializationRollbackOwnerDetached(program, rollbackOwner)
				) {
					// A terminal close consumed the failed generation completely.
				} else if (!closed && releasedExactOwner) {
					// Only the failed generation's one lease was released. The still-live
					// instance remains quarantined until stop closes its baseline owners.
					rollbackOwner.phase = "consumed";
					retainRollbackOwner = true;
				} else {
					const cleanupError = new Error(
						`Program at ${address} did not release its initialization rollback owner`,
					);
					cleanupErrors.push(cleanupError);
					retainRollbackOwner = true;
					logger.error(cleanupError.message);
				}
			} catch (cleanupError) {
				rollbackOwner.phase = terminalCommit(
					this,
					program,
					checkpoint,
					"close",
					state.parent,
				)
					? "committed"
					: "pending-pre-base";
				retainRollbackOwner = true;
				cleanupErrors.push(cleanupError);
				logger.error(
					`Failed to close partially opened program at ${address}: ${String(
						cleanupError,
					)}`,
				);
			} finally {
				this.recordInitializationRollbackPostCloseState(program, rollbackOwner);
			}
		} else {
			this.recordInitializationRollbackPostCloseState(program, rollbackOwner);
		}

		// beforeOpen() and application terminal callbacks can attach nested children
		// at any awaited rollback boundary. Consume every occurrence beyond the
		// baseline, including a forward-only edge hidden from program.children.
		await this.cleanupChildReferenceExcesses(
			address,
			program,
			rollbackOwner.children,
			rollbackOwner.childCandidates,
			cleanupErrors,
		);

		// Nested-child cleanup above can await arbitrary application code after the
		// close boundary. Re-capture immediately before the synchronous restore so
		// no owner or property identity adopted in that interval is erased.
		this.recordInitializationRollbackPostCloseState(program, rollbackOwner);
		this.restoreInitializationRollbackBaseline(program, rollbackOwner, false);
		this.restoreInitializationRollbackChildren(program, state.children);
		this.reconcileInitializationRollbackOwnerReferences(program, rollbackOwner);
		if (
			!retainRollbackOwner &&
			!this.initializationRollbackGraphDetached(program, rollbackOwner)
		) {
			retainRollbackOwner = true;
		}
		if (retainRollbackOwner) {
			this._initializationRollbackOwners.set(program, rollbackOwner);
		} else {
			this._initializationRollbackOwners.delete(program);
		}

		if (retainRollbackOwner || !terminalClosed(this, program)) {
			// Never manufacture a terminal state after close() failed. Keep the
			// partially initialized instance identity-tracked so later opens cannot
			// reuse it and stop() can retry cleanup.
			this.items.set(address, program as T);
			this._failedInitializations.set(address, program as T);
		} else {
			this._failedInitializations.delete(address);
		}

		if (cleanupErrors.length > 0) {
			logger.error(
				`Program initialization rollback at ${address} encountered ${cleanupErrors.length} cleanup error(s)`,
			);
		}
	}

	private dependencyGraph(program: T): Manageable<any>[] {
		const dependencies = this.properties.getDependencies?.(program) ?? [];
		return [...new Set<Manageable<any>>([program, ...dependencies])];
	}

	private async resolveDependencyAddresses(
		program: T,
	): Promise<Map<Manageable<any>, string>> {
		const addresses = new Map<Manageable<any>, string>();
		for (const candidate of this.dependencyGraph(program)) {
			const address = await candidate.save(
				this.properties.client.services.blocks,
				{
					skipOnAddress: true,
					save: () => false,
				},
			);
			addresses.set(candidate, address.toString());
		}
		return addresses;
	}

	private cleanupFailedOpeningMonitoring(
		reservation: OpeningReservation<T>,
	): void {
		for (const state of reservation.provisionalTerminalStates) {
			if (
				!terminalClosed(this, state.program) ||
				state.activeCalls > 0 ||
				state.pending.size > 0 ||
				state.failed ||
				state.cleanupLease ||
				state.program.pendingTerminalOperation != null ||
				this._cleanupResiduals.has(state.program)
			) {
				continue;
			}
			this.restoreTerminalOperations(state);
			if (this._terminalOperationsByAddress.get(state.address) === state) {
				this._terminalOperationsByAddress.delete(state.address);
			}
			this._terminalOperationsByProgram.delete(state.program);
		}
		reservation.provisionalTerminalStates.clear();
	}

	private releaseOpeningReservation(
		reservation: OpeningReservation<T>,
		failed = false,
	): void {
		for (const address of reservation.programsByAddress.keys()) {
			const reservations = this._openingReservationsByAddress.get(address);
			reservations?.delete(reservation);
			if (reservations?.size === 0) {
				this._openingReservationsByAddress.delete(address);
			}
		}
		if (failed) {
			for (const participant of reservation.contributedParticipants) {
				const references =
					reservation.group.participantReferences.get(participant) ?? 0;
				if (references <= 1) {
					reservation.group.participantReferences.delete(participant);
				} else {
					reservation.group.participantReferences.set(
						participant,
						references - 1,
					);
				}
			}
		}
		reservation.group.reservations.delete(reservation);
		this._openingReservations.delete(reservation);
		reservation.programsByAddress.clear();
		reservation.programs.clear();
		reservation.adoptedParents.clear();
		reservation.contributedParticipants.clear();
		reservation.wakeWaiters.clear();
		if (failed) {
			this.cleanupFailedOpeningMonitoring(reservation);
		} else {
			reservation.provisionalTerminalStates.clear();
		}
	}

	private reservationContains(
		reservation: OpeningReservation<T>,
		program: Manageable<any>,
	): boolean {
		return reservation.group.participantReferences.has(program);
	}

	private contributeOpeningParticipant(
		reservation: OpeningReservation<T>,
		program: Manageable<any>,
	): void {
		if (reservation.contributedParticipants.has(program)) return;
		reservation.contributedParticipants.add(program);
		reservation.group.participantReferences.set(
			program,
			(reservation.group.participantReferences.get(program) ?? 0) + 1,
		);
	}

	private mergeOpeningReservationGroups(
		left: OpeningReservation<T>,
		right: OpeningReservation<T>,
	): void {
		if (left.group === right.group) return;
		const target = left.group;
		const source = right.group;
		for (const [participant, references] of source.participantReferences) {
			target.participantReferences.set(
				participant,
				(target.participantReferences.get(participant) ?? 0) + references,
			);
		}
		for (const reservation of source.reservations) {
			reservation.group = target;
			target.reservations.add(reservation);
		}
		source.reservations.clear();
		source.participantReferences.clear();
	}

	private canShareOpeningReservation(
		existing: OpeningReservation<T>,
		reservation: OpeningReservation<T>,
		address: string,
		candidates: Set<Manageable<any>>,
		parent?: Manageable<any>,
	): boolean {
		const existingCandidates = existing.programsByAddress.get(address);
		if (
			!existingCandidates ||
			[...candidates].some((candidate) => !existingCandidates.has(candidate))
		) {
			return false;
		}
		if (existing.group === reservation.group) return true;
		if (parent && this.reservationContains(existing, parent)) return true;
		return [...reservation.adoptedParents].some((adoptedParent) =>
			this.reservationContains(existing, adoptedParent),
		);
	}

	private canOpenAlongsideReservation(
		existing: OpeningReservation<T>,
		address: string,
		candidates: Set<Manageable<any>>,
		rootProgram: Manageable<any>,
	): boolean {
		const existingCandidates = existing.programsByAddress.get(address);
		if (!existingCandidates) return false;
		if (
			[...candidates].some((candidate) => existingCandidates.has(candidate))
		) {
			return false;
		}
		return (
			!candidates.has(rootProgram) &&
			(existing.rootProgram == null ||
				!existingCandidates.has(existing.rootProgram))
		);
	}

	private adoptOpeningReservation(
		promise: Promise<T>,
		parent: Manageable<any>,
	): void {
		const reservation = this._openingReservationsByPromise.get(promise);
		if (!reservation || reservation.phase !== "opening") return;
		reservation.adoptedParents.add(parent);
		this.contributeOpeningParticipant(reservation, parent);
		for (const wake of [...reservation.wakeWaiters]) wake();
	}

	private async waitForOpeningConflictChange(
		reservation: OpeningReservation<T>,
		waits: Set<Promise<T>>,
	): Promise<void> {
		let wake!: () => void;
		const adopted = new Promise<void>((resolve) => {
			wake = resolve;
		});
		reservation.wakeWaiters.add(wake);
		try {
			await Promise.race([Promise.allSettled(waits), adopted]);
		} finally {
			reservation.wakeWaiters.delete(wake);
		}
	}

	private assertParentCanOwnOpen(parent: Manageable<any>): void {
		if (
			this._initializationRollbacks.has(parent) ||
			this._initializationRollbackOwners.has(parent)
		) {
			throw new Error(
				"Parent program is finishing initialization rollback cleanup",
			);
		}
		if (
			this._cleanupResiduals.has(parent) ||
			[...this._failedInitializations.values()].some(
				(failed) => (failed as Manageable<any>) === parent,
			)
		) {
			throw new Error("Parent program has failed lifecycle cleanup");
		}
		const terminalState = this._terminalOperationsByProgram.get(parent);
		if (
			(terminalState?.activeCalls ?? 0) > 0 ||
			terminalState?.failed ||
			parent.acceptsParentAttachments === false
		) {
			throw new Error("Parent program is finishing terminal cleanup");
		}
		if (terminalClosed(this, parent)) {
			throw new Error("Parent program is closed");
		}
	}

	private retainParentAttachment(parent: Manageable<any>): void {
		this._parentAttachmentReservations.set(
			parent,
			(this._parentAttachmentReservations.get(parent) ?? 0) + 1,
		);
	}

	private releaseParentAttachment(parent: Manageable<any>): void {
		const reservations = this._parentAttachmentReservations.get(parent) ?? 0;
		if (reservations <= 1) {
			this._parentAttachmentReservations.delete(parent);
		} else {
			this._parentAttachmentReservations.set(parent, reservations - 1);
		}
	}

	private isReservedNestedUse(
		address: string,
		program: Manageable<any> | undefined,
		parent: Manageable<any>,
	): boolean {
		return [...(this._openingReservationsByAddress.get(address) ?? [])].some(
			(reservation) =>
				this.reservationContains(reservation, parent) &&
				(program == null ||
					reservation.programsByAddress.get(address)?.has(program) === true),
		);
	}

	private async waitForOpeningReservations(
		address: string,
		program?: Manageable<any>,
		parent?: Manageable<any>,
		observedReservations: Set<OpeningReservation<T>> = new Set(),
	): Promise<void> {
		while (true) {
			const waits = new Set<Promise<T>>();
			for (const reservation of this._openingReservationsByAddress.get(
				address,
			) ?? []) {
				if (
					parent &&
					this.reservationContains(reservation, parent) &&
					(program == null ||
						reservation.programsByAddress.get(address)?.has(program) === true)
				) {
					continue;
				}
				if (
					reservation.phase === "rollback" &&
					!observedReservations.has(reservation)
				) {
					throw new Error(
						`Program at ${address} is finishing initialization rollback cleanup`,
					);
				}
				waits.add(reservation.promise);
			}
			if (waits.size === 0) return;
			await Promise.allSettled(waits);
		}
	}

	private async prepareOpeningGraph(
		program: T,
		reservation: OpeningReservation<T>,
		parent?: Manageable<any>,
		resolvedAddresses?: Map<Manageable<any>, string>,
	): Promise<Map<Manageable<any>, string>> {
		const addresses =
			resolvedAddresses ?? (await this.resolveDependencyAddresses(program));
		reservation.rootProgram = program;
		const programs = [...addresses.keys()];
		for (const candidate of programs) {
			this.contributeOpeningParticipant(reservation, candidate);
		}
		const programsByAddress = new Map<string, Set<Manageable<any>>>();
		for (const [candidate, address] of addresses) {
			const candidates = programsByAddress.get(address) ?? new Set();
			candidates.add(candidate);
			programsByAddress.set(address, candidates);
		}

		while (true) {
			const waits = new Set<Promise<T>>();
			for (const [address, candidates] of programsByAddress) {
				for (const existing of this._openingReservationsByAddress.get(
					address,
				) ?? []) {
					if (existing === reservation) continue;
					if (
						this.canShareOpeningReservation(
							existing,
							reservation,
							address,
							candidates,
							parent,
						)
					) {
						this.mergeOpeningReservationGroups(reservation, existing);
						continue;
					}
					if (
						existing.phase === "rollback" &&
						!reservation.observedReservations.has(existing)
					) {
						throw new Error(
							`Program at ${address} is finishing initialization rollback cleanup`,
						);
					}
					if (
						this.canOpenAlongsideReservation(
							existing,
							address,
							candidates,
							program,
						)
					) {
						continue;
					}
					const existingPrograms = existing.programsByAddress.get(address);
					if (
						existingPrograms &&
						![...candidates].some((candidate) =>
							existingPrograms.has(candidate),
						)
					) {
						throw new Error(
							`Program at ${address} is already opening as another instance`,
						);
					}
					waits.add(existing.promise);
				}
			}
			if (waits.size === 0) break;
			await this.waitForOpeningConflictChange(reservation, waits);
		}

		for (const [address, candidates] of programsByAddress) {
			this.assertNoFailedInitialization(address);
			for (const candidate of candidates) {
				this.assertNoTerminalOperation(
					address,
					candidate,
					candidate === program,
				);
			}
		}

		// No await separates the final conflict check from claiming every address.
		// Terminal admission reads the same map synchronously, so either the open
		// generation owns the whole graph or cleanup wins and the checks above fail.
		reservation.programs = new Set(programs);
		for (const [address, candidates] of programsByAddress) {
			for (const existing of this._openingReservationsByAddress.get(address) ??
				[]) {
				if (existing === reservation) continue;
				if (
					this.canShareOpeningReservation(
						existing,
						reservation,
						address,
						candidates,
						parent,
					)
				) {
					this.mergeOpeningReservationGroups(reservation, existing);
				} else if (
					this.canOpenAlongsideReservation(
						existing,
						address,
						candidates,
						program,
					)
				) {
					continue;
				} else {
					throw new TerminalOperationNotStartedError(
						`Program at ${address} became reserved by another open generation`,
					);
				}
			}
			reservation.programsByAddress.set(address, new Set(candidates));
			const reservations =
				this._openingReservationsByAddress.get(address) ?? new Set();
			reservations.add(reservation);
			this._openingReservationsByAddress.set(address, reservations);
		}
		for (const [address, candidates] of programsByAddress) {
			for (const candidate of candidates) {
				const monitoring = this.monitorTerminalOperations(
					address,
					candidate as T,
				);
				if (monitoring.newLifecycle) {
					reservation.provisionalTerminalStates.add(monitoring.state);
				}
			}
		}
		return addresses;
	}

	private trackOpening<S extends T>(
		address: string,
		open: (reservation: OpeningReservation<T>) => Promise<S>,
		observedReservations: Set<OpeningReservation<T>>,
	): Promise<S> {
		let resolve!: (program: S) => void;
		let reject!: (error: unknown) => void;
		const openPromise = new Promise<S>((promiseResolve, promiseReject) => {
			resolve = promiseResolve;
			reject = promiseReject;
		});
		const group: OpeningReservationGroup = {
			participantReferences: new Map(),
			reservations: new Set(),
		};
		const reservation: OpeningReservation<T> = {
			promise: openPromise as Promise<T>,
			phase: "opening",
			observedReservations,
			adoptedParents: new Set(),
			contributedParticipants: new Set(),
			wakeWaiters: new Set(),
			group,
			programsByAddress: new Map(),
			programs: new Set(),
			provisionalTerminalStates: new Set(),
		};
		group.reservations.add(reservation);
		this._openingReservations.add(reservation);
		this._openingReservationsByPromise.set(
			openPromise as Promise<T>,
			reservation,
		);
		this._openingPromises.set(address, openPromise as Promise<T>);
		let execution: Promise<S>;
		try {
			execution = open(reservation);
		} catch (error) {
			execution = Promise.reject(error);
		}
		void execution.then(
			(program) => {
				this.releaseOpeningReservation(reservation);
				if (this._openingPromises.get(address) === openPromise) {
					this._openingPromises.delete(address);
				}
				resolve(program);
			},
			(error) => {
				this.releaseOpeningReservation(reservation, true);
				if (this._openingPromises.get(address) === openPromise) {
					this._openingPromises.delete(address);
				}
				reject(error);
			},
		);
		return openPromise;
	}

	async open<S extends T>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<S> = {},
	): Promise<S> {
		if (!this._acceptingOpens) {
			throw new Error("Program handler is stopping or stopped");
		}
		const attachmentParent =
			options.parent && options.parent !== storeOrAddress
				? options.parent
				: undefined;
		if (attachmentParent) {
			this.assertParentCanOwnOpen(attachmentParent);
			this.retainParentAttachment(attachmentParent);
		}
		const observedReservations = new Set(
			[...this._openingReservations].filter(
				(reservation) => reservation.phase === "opening",
			),
		);
		let completeAdmission!: () => void;
		const admission = new Promise<void>((resolve) => {
			completeAdmission = resolve;
		});
		this._openAdmissions.add(admission);
		try {
			return await this.openAdmitted(
				storeOrAddress,
				options,
				observedReservations,
			);
		} finally {
			if (attachmentParent) {
				this.releaseParentAttachment(attachmentParent);
			}
			this._openAdmissions.delete(admission);
			completeAdmission();
		}
	}

	private async openAdmitted<S extends T>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<S> = {},
		observedReservations: Set<OpeningReservation<T>> = new Set(),
	): Promise<S> {
		if (
			typeof storeOrAddress !== "string" &&
			this._cleanupResiduals.has(storeOrAddress)
		) {
			throw new Error(
				`Program at ${storeOrAddress.address} has pending cleanup residuals and cannot be reopened before cleanup is retried`,
			);
		}
		if (options.parent && options.parent !== storeOrAddress) {
			this.assertParentCanOwnOpen(options.parent);
		}
		// Parent opens historically share subprograms by default. Explicit parent
		// strategies still have to retain their reject/replace semantics.
		const mergeStrategy: ProgramMergeStrategy | undefined = options.parent
			? (options.existing ?? "reuse")
			: options.existing;
		const fn = async (
			openingReservation?: OpeningReservation<T>,
		): Promise<S> => {
			// TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?
			let program = storeOrAddress as S;
			if (typeof storeOrAddress === "string") {
				const address = storeOrAddress.toString();
				try {
					this.assertNoFailedInitialization(address);
					this.assertNoTerminalOperation(address);
					const existing = this.findMonitoredProgram(address);
					if (existing) {
						// Be defensive: stale handles shouldn't be returned from the cache.
						if (terminalClosed(this, existing)) {
							if (this.items.get(address) === existing) {
								this.items.delete(address);
							}
						} else if (mergeStrategy === "reuse") {
							return this.attachExistingProgram(
								address,
								existing,
								options.parent,
							) as S;
						} else if (mergeStrategy === "replace") {
							await this.closeForReplacement(address, existing, options.parent);
						} else {
							throw new Error(`Program at ${address} is already open`);
						}
					}

					program = (await this.properties.load(
						address,
						this.properties.client.services.blocks,
						options,
					)) as S; // TODO fix typings

					if (!this.properties.shouldMonitor(program)) {
						if (!program) {
							throw new Error(
								"Failed to resolve program with address: " + address,
							);
						}
						throw new Error(
							`Failed to open program because program is of type ${program?.constructor.name} `,
						);
					}
				} catch (error) {
					logger.error(
						"Failed to load store with address: " + storeOrAddress.toString(),
					);
					throw error;
				}
			} else {
				if (options.parent === program) {
					throw new Error("Parent program can not be equal to the program");
				}

				if (!terminalClosed(this, program)) {
					this.assertNoFailedInitialization(program.address);
					this.assertNoTerminalOperation(program.address, program);
					const existing = this.findMonitoredProgram(program.address);
					if (existing === program) {
						return this.attachExistingProgram(
							program.address,
							existing,
							options.parent,
						) as S;
					} else if (existing) {
						// we got existing, but it is not the same instance
						const existing = await this.checkProcessExisting(
							program.address,
							program,
							mergeStrategy,
							options.parent,
						);

						if (existing) {
							return existing as S;
						}
					} else {
						if (
							!program.node.identity.publicKey.equals(
								this.properties.identity.publicKey,
							)
						) {
							throw new Error(
								`Program at ${program.address} is already opened with a different client`,
							);
						}
						assertCanAttachParent(program);
						if (!openingReservation) {
							throw new Error("Missing opening graph reservation");
						}
						resolvedDependencyAddresses = await this.prepareOpeningGraph(
							program,
							openingReservation,
							options.parent,
							resolvedDependencyAddresses,
						);

						// assume new instance was not added to monitored items, just add it
						// and return it as we would opened it normally

						await program.save(this.properties.client.services.blocks, {
							skipOnAddress: false,
							save: (address) => {
								return !this.findMonitoredProgram(address, true);
							},
						});
						assertCanAttachParent(program);
						if (options.parent) {
							this.assertParentCanOwnOpen(options.parent);
						}
						this.monitorTerminalOperations(program.address, program);
						this.items.set(program.address, program);
						addParent(program, options.parent);
						return program;
					}
				}
			}

			const existingBeforeSave = await this.checkProcessExisting(
				program.address,
				program,
				mergeStrategy,
				options.parent,
			);
			if (existingBeforeSave) {
				return existingBeforeSave as S;
			}
			if (!openingReservation) {
				throw new Error("Missing opening graph reservation");
			}
			resolvedDependencyAddresses = await this.prepareOpeningGraph(
				program,
				openingReservation,
				options.parent,
				resolvedDependencyAddresses,
			);

			logger.trace(`Open database '${program.constructor.name}`);

			// todo make this nicer
			let address = await program.save(this.properties.client.services.blocks, {
				skipOnAddress: !resolvedWithoutSaving,
				save: (address) => {
					return !this.findMonitoredProgram(address, true);
				},
			});

			const existing = await this.checkProcessExisting(
				address,
				program,
				mergeStrategy,
				options.parent,
			);
			if (existing) {
				return existing as S;
			}

			const parentChildrenState = options.parent
				? this.captureArrayPropertyState<Manageable<any>>(
						options.parent,
						"children",
					)
				: undefined;
			const rollbackState = {
				parents: this.captureArrayProperty<Manageable<any> | undefined>(
					program,
					"parents",
				),
				children: this.captureArrayProperty<Manageable<any>>(
					program,
					"children",
				),
				parent: options.parent,
				parentChildren:
					parentChildrenState == null
						? undefined
						: {
								hadOwnProperty: parentChildrenState.hadOwnProperty,
								value: parentChildrenState.value,
							},
				parentChildrenWorkingReference: parentChildrenState?.reference,
				parentChildrenWorkingSnapshot: options.parent
					? {
							hadOwnProperty: true,
							value: [...(parentChildrenState?.value ?? []), program],
						}
					: undefined,
			};
			this.monitorTerminalOperations(address, program);
			try {
				const {
					onBeforeOpen: userOnBeforeOpen,
					onOpen: userOnOpen,
					onClose: userOnClose,
					onDrop: userOnDrop,
					...programOptions
				} = options;
				if (options.parent) {
					this.assertParentCanOwnOpen(options.parent);
				}
				await this.invokeLifecycleMethod(() =>
					program.beforeOpen(this.properties.client, {
						...programOptions,
						onBeforeOpen: async (p) => {
							if (options.parent) {
								rollbackState.parentChildrenWorkingReference =
									options.parent.children;
								rollbackState.parentChildrenWorkingSnapshot =
									this.captureArrayProperty<Manageable<any>>(
										options.parent,
										"children",
									);
							}
							if (this.properties.shouldMonitor(program)) {
								this.items.set(address, program);
							}
							if (userOnBeforeOpen) {
								this._openLifecycleCallbacks += 1;
								try {
									await userOnBeforeOpen(p);
								} finally {
									this._openLifecycleCallbacks -= 1;
								}
							}
						},
						onOpen: async (p) => {
							if (!userOnOpen) return;
							this._openLifecycleCallbacks += 1;
							try {
								await userOnOpen(p);
							} finally {
								this._openLifecycleCallbacks -= 1;
							}
						},
						onClose: async (p) => {
							if (this.properties.shouldMonitor(p)) {
								this._onProgamClose(p as T); // TODO types
							}
							if (userOnClose) {
								this._terminalLifecycleCallbacks += 1;
								try {
									await userOnClose(p);
								} finally {
									this._terminalLifecycleCallbacks -= 1;
								}
							}
						},
						onDrop: async (p) => {
							if (this.properties.shouldMonitor(p)) {
								this._onProgamClose(p);
							}
							if (userOnDrop) {
								this._terminalLifecycleCallbacks += 1;
								try {
									await userOnDrop(p);
								} finally {
									this._terminalLifecycleCallbacks -= 1;
								}
							}
						},
						// If the program opens more programs
						// reset: options.reset,
					}),
				);
				await this.invokeLifecycleMethod(() => program.open(options.args));
				await this.invokeLifecycleMethod(() => program.afterOpen());
				return program as S;
			} catch (error) {
				try {
					if (openingReservation) {
						openingReservation.phase = "rollback";
					}
					const rollbackPrograms = new Set<Manageable<any>>([
						program,
						...(resolvedDependencyAddresses?.keys() ?? []),
					]);
					for (const rollbackProgram of rollbackPrograms) {
						this._initializationRollbacks.add(rollbackProgram);
					}
					try {
						await this.rollbackFailedInitialization(
							address,
							program,
							rollbackState,
							rollbackPrograms,
						);
					} finally {
						for (const rollbackProgram of rollbackPrograms) {
							this._initializationRollbacks.delete(rollbackProgram);
						}
					}
				} catch (cleanupError) {
					logger.error(
						`Failed to roll back program at ${address}: ${String(cleanupError)}`,
					);
				}
				throw error;
			}
		};

		// Helper to resolve address from storeOrAddress
		let resolvedWithoutSaving = false;
		let resolvedDependencyAddresses: Map<Manageable<any>, string> | undefined;
		const resolveAddress = async (): Promise<string> => {
			if (typeof storeOrAddress === "string") {
				return storeOrAddress;
			}
			resolvedWithoutSaving = terminalClosed(this, storeOrAddress);
			resolvedDependencyAddresses =
				await this.resolveDependencyAddresses(storeOrAddress);
			const address = resolvedDependencyAddresses.get(storeOrAddress);
			if (!address) {
				throw new Error("Failed to resolve the root program address");
			}
			return address;
		};

		const address = await resolveAddress();
		await this.waitForOpeningReservations(
			address,
			typeof storeOrAddress === "string" ? undefined : storeOrAddress,
			options.parent,
			observedReservations,
		);
		if (!this._openingPromises.has(address)) {
			this.assertNoFailedInitialization(address);
			this.assertNoTerminalOperation(
				address,
				typeof storeOrAddress === "string" ? undefined : storeOrAddress,
			);
		}

		// For parent opens, check if already opened or in-progress and return early
		// This prevents race conditions while avoiding deadlocks
		if (options?.parent) {
			while (true) {
				// Check if there's an open in progress FIRST - wait for it. This
				// must precede items because beforeOpen() adds to items before open()
				// completes.
				const existingPromise = this._openingPromises.get(address);
				if (existingPromise) {
					const requestedProgram =
						typeof storeOrAddress === "string" ? undefined : storeOrAddress;
					this.adoptOpeningReservation(existingPromise, options.parent);
					if (
						this.isReservedNestedUse(address, requestedProgram, options.parent)
					) {
						const existing = await this.processParentExisting<S>(
							address,
							requestedProgram,
							mergeStrategy ?? "reuse",
							options.parent,
						);
						if (existing) return existing;
					}
					try {
						await existingPromise;
					} catch {
						// The failed generation has completed its identity-safe rollback.
						// This admitted request still owns its independent open semantics.
					}
					continue;
				}

				const existing = await this.processParentExisting<S>(
					address,
					typeof storeOrAddress === "string" ? undefined : storeOrAddress,
					mergeStrategy ?? "reuse",
					options.parent,
				);
				if (existing) {
					return existing;
				}

				// processParentExisting() can await replacement and even its empty
				// async path yields once. Re-read both gates after that yield. With no
				// await between this final check and trackOpening(), registering a new
				// parent generation is an atomic singleflight claim on this JS turn.
				if (this._openingPromises.has(address) || this.items.has(address)) {
					continue;
				}
				return this.trackOpening(
					address,
					(reservation) => fn(reservation),
					observedReservations,
				);
			}
		}

		// Non-parent opens use queue for serialization
		let queue = this._openQueue.get(address);
		if (!queue) {
			queue = new PQueue({ concurrency: 1 });
			this._openQueue.set(address, queue);
		}
		return queue.add(async () => {
			while (true) {
				// Parent opens bypass the queue to avoid nested-open deadlocks. Wait for
				// such a generation here before applying this root open's own
				// reject/reuse/replace semantics. Loop so a newer parent generation that
				// claimed the address while this waiter resumed is also observed.
				const existingPromise = this._openingPromises.get(address);
				if (existingPromise) {
					try {
						await existingPromise;
					} catch {
						// Re-read Handler state and apply this root request's own strategy
						// after the failed generation has rolled back.
					}
					continue;
				}

				// Existing live non-replacement operations finish from Handler state
				// and do not represent a new initialization generation. In particular,
				// don't make a concurrent parent inherit a duplicate-open rejection. A
				// stale closed cache entry, however, is evicted before fn() starts the
				// new generation visible to parent opens.
				const existing = this.findMonitoredProgram(address);
				if (existing && terminalClosed(this, existing)) {
					this.assertNoTerminalOperation(
						address,
						typeof storeOrAddress === "string" ? undefined : storeOrAddress,
					);
					if (this.items.get(address) === existing) {
						this.items.delete(address);
					}
				} else if (existing && mergeStrategy !== "replace") {
					return fn();
				}

				// No await separates the empty gate from registration, so a parent
				// open cannot claim this address in between.
				return this.trackOpening(
					address,
					(reservation) => fn(reservation),
					observedReservations,
				);
			}
		}) as any as S;
	}
}
