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
	cleanupLease?: object;
};

type CleanupResidual = {
	program: Manageable<any>;
	failures: FailedTerminalCall[];
	cleanupLease?: object;
	activeReservations: number;
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
	private _replacementClosures: WeakSet<Manageable<any>>;

	constructor(
		readonly properties: {
			client: { services: { blocks: Blocks }; stop: () => Promise<void> };
			load: (
				address: Address,
				blocks: Blocks,
				options?: { timeout?: number },
			) => Promise<T | undefined>;
			shouldMonitor: (thing: any) => boolean;
			identity: Identity;
			getDependencies?: (program: T) => Manageable<any>[];
		},
	) {
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

		if (program.close !== state.closeWrapper) {
			const close = program.close;
			state.closeOperation = close;
			const closeWrapper = (...args: any[]) =>
				this.runTerminalOperation(state!, "close", close, args);
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
				this.runTerminalOperation(state!, "drop", drop, args);
			state.dropWrapper = dropWrapper;
			droppable.drop = dropWrapper;
		}
		return { state, newLifecycle };
	}

	private runTerminalOperation(
		state: TerminalOperationState<T>,
		type: TerminalOperation,
		operation: (...args: any[]) => Promise<boolean>,
		args: any[],
	): Promise<boolean> {
		const from = args[0] as Manageable<any> | undefined;
		if (
			state.program.terminalLifecycleCallbackRunning ||
			(this._terminalLifecycleCallbacks > 0 && state.activeCalls > 0)
		) {
			return Promise.reject(
				new TerminalOperationNotStartedError(
					"Program lifecycle callbacks cannot wait for their own terminal operation",
				),
			);
		}
		const parentIndex =
			state.program.parents?.findIndex((parent) => parent === from) ?? -1;
		const terminal =
			state.program.closed ||
			parentIndex === -1 ||
			(state.program.parents?.length ?? 0) === 1;
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
			this.assertTerminalGraphCanStart(state.program);
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
			state.program.parents?.filter((parent) => parent === from).length ?? 0;
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
			promise: tracked,
		};
		const hasPredecessor = state.outerCalls.size > 0;
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

			const startedClosed = state.program.closed;
			const startedAcceptingParents = state.program.acceptsParentAttachments;
			const startedParents = [...(state.program.parents ?? [])];
			const checkpoint = this.terminalCheckpoint(state.program);
			const existingCleanupLease = state.cleanupLease;
			if (!state.cleanupLease) {
				state.cleanupLease = state.program[TERMINAL_OUTER_CLEANUP_RETAIN]?.();
			}
			const releaseProvisionalCleanupLease = () => {
				if (!existingCleanupLease && state.cleanupLease) {
					state.program[TERMINAL_OUTER_CLEANUP_RELEASE]?.(state.cleanupLease);
					state.cleanupLease = undefined;
				}
			};
			try {
				const invoke = () => {
					outerCall.ownerReferencesBeforeInvoke =
						state.program.parents?.filter((parent) => parent === from).length ??
						0;
					let operationResult: Promise<boolean>;
					try {
						operationResult = this.invokeLifecycleMethod(() =>
							operation.apply(state.program, args),
						);
					} finally {
						outerCall.ownerReferencesAfterInvoke =
							state.program.parents?.filter((parent) => parent === from)
								.length ?? 0;
					}
					return operationResult!;
				};
				const result =
					recovering && failedCall?.commit
						? await this.retryCommittedTerminalCall(
								state.program,
								failedCall.commit,
								invoke,
							)
						: await invoke();
				if (recovering) {
					const recoveryOwner = failedCall?.args[0] as
						| Manageable<any>
						| undefined;
					const releasedDuringRetry = Math.max(
						0,
						startedParents.filter((parent) => parent === recoveryOwner).length -
							(state.program.parents?.filter(
								(parent) => parent === recoveryOwner,
							).length ?? 0),
					);
					this.removeReleasedParentChildReferences(
						state.program,
						recoveryOwner,
						Math.max(
							failedCall?.commit?.releasedParentReferences ??
								failedCall?.releasedParentReferences ??
								0,
							releasedDuringRetry,
						),
					);
					state.failed = false;
					state.failedOperation = undefined;
					state.failedCall = undefined;
				} else {
					const commit = this.terminalCommit(
						state.program,
						checkpoint,
						type,
						from,
					);
					this.removeReleasedParentChildReferences(
						state.program,
						from,
						commit?.releasedParentReferences ?? 0,
					);
				}
				if (result && state.program.closed && !state.failed) {
					state.terminalCompleted = true;
				}
				if (state.cleanupLease) {
					state.program[TERMINAL_OUTER_CLEANUP_RELEASE]?.(state.cleanupLease);
					state.cleanupLease = undefined;
				}
				return result;
			} catch (error) {
				const observedCommit =
					failedCall?.commit ||
					this.terminalCommit(state.program, checkpoint, type, from);
				if (
					!recovering &&
					error instanceof TerminalOperationNotStartedError &&
					!observedCommit
				) {
					// Exclude this call's provisional lease while validating that the
					// explicit precondition error made no lifecycle mutation.
					releaseProvisionalCleanupLease();
					if (
						state.program.closed === startedClosed &&
						state.program.acceptsParentAttachments ===
							startedAcceptingParents &&
						this.sameParents(state.program.parents ?? [], startedParents)
					) {
						state.terminalCompleted = startedClosed;
						throw error;
					}
					state.cleanupLease = state.program[TERMINAL_OUTER_CLEANUP_RETAIN]?.();
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
					state.program.closed !== startedClosed ||
					!this.sameParents(state.program.parents ?? [], startedParents);
				const recoveryType: TerminalOperation =
					type === "drop" && !baseProgressed
						? "drop"
						: commit || state.program.closed
							? type
							: "close";
				const releasedParentReferences = Math.max(
					failedCall?.releasedParentReferences ?? 0,
					startedParents.filter((parent) => parent === from).length -
						(state.program.parents?.filter((parent) => parent === from)
							.length ?? 0),
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
				this.items.set(state.address, state.program);
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

	private terminalCheckpoint(program: Manageable<any>): number {
		return program[TERMINAL_BASE_CHECKPOINT]?.() ?? 0;
	}

	private terminalCommit(
		program: Manageable<any>,
		checkpoint: number,
		type: TerminalOperation,
		from?: Manageable<any>,
	): TerminalBaseCommit | undefined {
		return program[TERMINAL_BASE_COMMIT]?.(checkpoint, type, from);
	}

	private retryCommittedTerminalCall(
		program: Manageable<any>,
		commit: TerminalBaseCommit,
		operation: () => Promise<boolean>,
	): Promise<boolean> {
		return program[TERMINAL_BASE_RETRY]
			? program[TERMINAL_BASE_RETRY](commit, operation)
			: operation();
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
			program[TERMINAL_OUTER_CLEANUP_RELEASE]?.(cleanupLease);
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
			cleanupLease ?? program[TERMINAL_OUTER_CLEANUP_RETAIN]?.();
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
		if (residual.activeReservations === 0 && residual.failures.length === 0) {
			this.releaseCleanupResidual(residual);
		}
	}

	private releaseCleanupResidual(residual: CleanupResidual): void {
		if (residual.activeReservations > 0) {
			throw new Error(
				"Cleanup residual cannot be released while rollback cleanup is active",
			);
		}
		if (residual.cleanupLease) {
			residual.program[TERMINAL_OUTER_CLEANUP_RELEASE]?.(residual.cleanupLease);
			residual.cleanupLease = undefined;
		}
		this._cleanupResiduals.delete(residual.program);
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
		const checkpoint = this.terminalCheckpoint(residual.program);
		const invoke = () =>
			this.invokeLifecycleMethod(() =>
				operation.apply(residual.program, failure.args),
			);
		try {
			if (failure.commit) {
				await this.retryCommittedTerminalCall(
					residual.program,
					failure.commit,
					invoke,
				);
			} else {
				await invoke();
			}
		} catch (error) {
			failure.commit ??= this.terminalCommit(
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
			!state.program.closed
		) {
			return;
		}
		if (this.items.get(state.address) === state.program) {
			this.items.delete(state.address);
		}
		this.restoreTerminalOperations(state);
		if (this._failedInitializations.get(state.address) === state.program) {
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
			(!liveOnly || !preferred.closed)
		) {
			return preferred as T;
		}
		return liveOnly
			? matches.find((candidate) => !candidate.closed)
			: (matches.find((candidate) => !candidate.closed) ?? matches[0]);
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
			if (direct && !direct.closed && direct !== program) {
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
				(!candidate.closed ||
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

	private async closeCompletely(program: Manageable<any>): Promise<void> {
		await this.waitForTerminalOperations(program);
		let terminalState = this._terminalOperationsByProgram.get(program);
		while (!program.closed || terminalState?.failed) {
			const wasRecovering = terminalState?.failed === true;
			const failedCall = terminalState?.failedCall;
			const ownersBefore = [...(program.parents ?? [])];
			const owner = wasRecovering
				? (failedCall?.args[0] as Manageable<any> | undefined)
				: program.closed
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
			terminalState = this._terminalOperationsByProgram.get(program);
			if (
				closed &&
				program.closed &&
				terminalState?.failed &&
				terminalState.failedOperation === operationType
			) {
				// This also supports a test/application replacing the wrapped method
				// after open: closeCompletely awaited the full replacement call, so it
				// is safe to mark the previously retained cleanup as recovered.
				terminalState.failed = false;
				terminalState.failedOperation = undefined;
				terminalState.failedCall = undefined;
				terminalState.terminalCompleted = true;
				if (terminalState.cleanupLease) {
					program[TERMINAL_OUTER_CLEANUP_RELEASE]?.(terminalState.cleanupLease);
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
			if (program.closed && !terminalState?.failed) {
				return;
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

	private assertNoFailedInitialization(address: Address) {
		const failed = this._failedInitializations.get(address);
		if (!failed) {
			return;
		}
		if (failed.closed) {
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

		// A close can legitimately return false when it only releases one of several
		// owners. Drain every ownership reference so stop cannot discard a still-live
		// program from Handler state.
		const closeErrors: unknown[] = [];
		for (const [address, program] of [...this.items]) {
			try {
				await this.closeCompletely(program);
			} catch (error) {
				closeErrors.push(error);
			}
			const terminalState = this._terminalOperationsByProgram.get(program);
			if (
				program.closed &&
				!terminalState?.failed &&
				(terminalState?.activeCalls ?? 0) === 0 &&
				this.items.get(address) === program
			) {
				this.items.delete(address);
			}
			if (
				program.closed &&
				!terminalState?.failed &&
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
				program.closed &&
				residual.failures.length === 0 &&
				!terminalState?.failed &&
				(terminalState?.activeCalls ?? 0) === 0
			) {
				this.releaseCleanupResidual(residual);
			}
		}
		if (closeErrors.length > 0) {
			throw closeErrors[0];
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
		if (this._failedInitializations.get(address) === program) {
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
		if (!closed || !program.closed || parentReferencesAfter > 0) {
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
		if (prev?.closed) {
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
		if (existing?.closed) {
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
			await existing.save(this.properties.client.services.blocks, {
				skipOnAddress: false,
			});
		} catch (error) {
			const parentIndex = existing.parents?.lastIndexOf(parent) ?? -1;
			if (parentIndex !== -1) existing.parents.splice(parentIndex, 1);
			const childIndex = parent.children?.lastIndexOf(existing) ?? -1;
			if (childIndex !== -1) parent.children.splice(childIndex, 1);
			throw error;
		}
		return existing as S;
	}

	private async rollbackFailedInitialization(
		address: Address,
		program: Manageable<any>,
		state: {
			parents?: (Manageable<any> | undefined)[];
			children?: Manageable<any>[];
			parent?: Manageable<any>;
			parentChildReferences: number;
		},
	): Promise<void> {
		if (this.items.get(address) === program) {
			this.items.delete(address);
		}

		const cleanupErrors: unknown[] = [];
		const childrenAfterFailure = [...(program.children ?? [])];
		try {
			if (!program.closed) {
				await program.close();
			}
		} catch (cleanupError) {
			cleanupErrors.push(cleanupError);
			logger.error(
				`Failed to close partially opened program at ${address}: ${String(
					cleanupError,
				)}`,
			);
		}

		// beforeOpen() can attach nested children before the parent itself becomes
		// open. Remove only relationships added by this failed generation, then
		// restore the exact caller-visible parent/child arrays captured at entry.
		const baselineChildCounts = new Map<Manageable<any>, number>();
		for (const child of state.children ?? []) {
			baselineChildCounts.set(child, (baselineChildCounts.get(child) ?? 0) + 1);
		}
		const currentChildCounts = new Map<Manageable<any>, number>();
		for (const child of childrenAfterFailure) {
			currentChildCounts.set(child, (currentChildCounts.get(child) ?? 0) + 1);
		}
		for (const [child, currentCount] of currentChildCounts) {
			const baselineCount = baselineChildCounts.get(child) ?? 0;
			let extra = currentCount - baselineCount;
			while (extra > 0) {
				const referencesBefore =
					child.parents?.filter((parent) => parent === program).length ?? 0;
				let retainedFailure = false;
				let cleanupReservation: CleanupResidual | undefined;
				if (!child.closed && referencesBefore > 0) {
					const checkpoint = this.terminalCheckpoint(child);
					cleanupReservation = this.acquireCleanupReservation(child);
					try {
						await (
							child.close as unknown as (
								from?: Manageable<any>,
							) => Promise<boolean>
						).call(child, program);
					} catch (cleanupError) {
						retainedFailure = true;
						const terminalState = this._terminalOperationsByProgram.get(
							child as T,
						);
						const handlerOwnsFailure =
							terminalState?.failed === true &&
							terminalState.failedCall?.type === "close" &&
							this.sameTerminalArgs(terminalState.failedCall.args, [program]);
						if (handlerOwnsFailure) {
						} else {
							cleanupReservation.failures.push({
								type: "close",
								args: [program],
								commit: this.terminalCommit(
									child,
									checkpoint,
									"close",
									program,
								),
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
				extra -= 1;
			}
			const remainingProgramReferences =
				child.parents?.filter((parent) => parent === program).length ?? 0;
			if (
				!child.closed &&
				baselineCount === 0 &&
				(remainingProgramReferences > 0 || (child.parents?.length ?? 0) === 0)
			) {
				// A failed nested cleanup must remain reachable by stop(); restoring the
				// caller-visible arrays alone would otherwise orphan a live resource.
				if (!this._cleanupResiduals.has(child)) {
					this.addCleanupResidual(child, {
						type: "close",
						args: [remainingProgramReferences > 0 ? program : undefined],
					});
				}
			}
		}

		(program as { parents?: (Manageable<any> | undefined)[] }).parents =
			state.parents ? [...state.parents] : undefined;
		(program as { children?: Manageable<any>[] }).children = state.children
			? [...state.children]
			: undefined;
		if (state.parent) {
			let currentReferences =
				state.parent.children?.filter((child) => child === program).length ?? 0;
			while (currentReferences > state.parentChildReferences) {
				const childIndex = state.parent.children.lastIndexOf(program);
				if (childIndex === -1) {
					break;
				}
				state.parent.children.splice(childIndex, 1);
				currentReferences -= 1;
			}
		}

		if (!program.closed) {
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
				!state.program.closed ||
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
		if (this._initializationRollbacks.has(parent)) {
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
		if (parent.closed) {
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
						if (existing.closed) {
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

				if (!program.closed) {
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

			const rollbackState = {
				parents: program.parents ? [...program.parents] : undefined,
				children: program.children ? [...program.children] : undefined,
				parent: options.parent,
				parentChildReferences:
					options.parent?.children?.filter((child) => child === program)
						.length ?? 0,
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
			resolvedWithoutSaving = storeOrAddress.closed;
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
				if (existing?.closed) {
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
