import type { Identity, PublicSignKey } from "@peerbit/crypto";
import type { OpenOptions, Program, ProgramEvents } from "@peerbit/program";
import { useEffect, useReducer, useRef, useState } from "react";

type ExtractArgs<T> = T extends Program<infer Args> ? Args : never;
type ExtractEvents<T> = T extends Program<any, infer Events> ? Events : never;

export type ProgramClientLike = {
	identity: Identity<PublicSignKey>;
	open: <P extends Program<any, ProgramEvents>>(
		addressOrOpen: P | string,
		options?: OpenOptions<P>,
	) => Promise<P>;
};

export type UseProgramStatus = "idle" | "loading" | "ready" | "error";

const UNSAVED_TARGET_KEY = Object.freeze({});

type ProgramRequest<P> = {
	peer: ProgramClientLike;
	target: P | string;
	targetKey: string | object;
	id: string | undefined;
	keepOpenOnUnmount: boolean;
};

const isSameRequest = <P,>(
	left: ProgramRequest<P> | undefined,
	right: ProgramRequest<P> | undefined,
) =>
	left === right ||
	(!!left &&
		!!right &&
		left.peer === right.peer &&
		left.targetKey === right.targetKey &&
		left.id === right.id &&
		left.keepOpenOnUnmount === right.keepOpenOnUnmount);

type ProgramView<P> = {
	request: ProgramRequest<P>;
	program?: P;
	loading: boolean;
	error?: Error;
	peers: PublicSignKey[];
};

type ProgramSession<P> = {
	generation: number;
	request: ProgramRequest<P>;
	active: boolean;
	keepOpenOnUnmount: boolean;
	openPromise: Promise<P | undefined>;
	program?: P;
	unsubscribe?: () => void;
	cleanupComplete: boolean;
	cleanupPromise?: Promise<void>;
};

type ProgramLoading<P> = {
	request: ProgramRequest<P>;
	owner: ProgramSession<P>;
	promise: Promise<P | undefined>;
};

export function useProgram<
	P extends Program<ExtractArgs<P>, ExtractEvents<P>> &
		Program<any, ProgramEvents>,
>(
	peer: ProgramClientLike | undefined,
	addressOrOpen?: P | string,
	options?: OpenOptions<P> & {
		id?: string;
		keepOpenOnUnmount?: boolean;
	},
) {
	const keepOpenOnUnmount = options?.keepOpenOnUnmount === true;
	const objectTargetKeysRef = useRef<WeakMap<object, string | object>>(
		new WeakMap(),
	);
	// Preserve address equivalence for already-addressed programs, but pin the
	// shared legacy unsaved key so inline constructors remain stable. options.id
	// is the explicit generation key when callers intentionally swap unsaved targets.
	const targetKey = (() => {
		if (typeof addressOrOpen === "string") return addressOrOpen;
		if (!addressOrOpen) return undefined;
		const cached = objectTargetKeysRef.current.get(addressOrOpen);
		if (cached !== undefined) return cached;

		let key: string | object = UNSAVED_TARGET_KEY;
		try {
			const address = addressOrOpen.address;
			if (address) {
				key = address;
			}
		} catch {}
		objectTargetKeysRef.current.set(addressOrOpen, key);
		return key;
	})();
	const currentRequest: ProgramRequest<P> | undefined =
		peer && addressOrOpen && targetKey
			? {
					peer,
					target: addressOrOpen,
					targetKey,
					id: options?.id,
					keepOpenOnUnmount,
				}
			: undefined;
	const [view, setView] = useState<ProgramView<P> | undefined>();
	const [resolvedId, setResolvedId] = useState<
		{ request: ProgramRequest<P>; value: string | undefined } | undefined
	>();
	const [session, forceUpdate] = useReducer((x) => x + 1, 0);
	const programLoadingRef = useRef<ProgramLoading<P> | undefined>(undefined);
	const currentSessionRef = useRef<ProgramSession<P> | undefined>(undefined);
	const generationRef = useRef(0);
	const lifecycleTailRef = useRef<Promise<void>>(Promise.resolve());
	const cleanupDebtRef = useRef<ProgramSession<P>[]>([]);

	const isCurrentSession = (candidate: ProgramSession<P>) =>
		candidate.active &&
		currentSessionRef.current === candidate &&
		generationRef.current === candidate.generation;

	const detachListener = (candidate: ProgramSession<P>) => {
		const unsubscribe = candidate.unsubscribe;
		candidate.unsubscribe = undefined;
		try {
			unsubscribe?.();
		} catch (error) {
			console.error("failed to remove program listeners", error);
		}
	};

	const removeCleanupDebt = (candidate: ProgramSession<P>) => {
		const index = cleanupDebtRef.current.indexOf(candidate);
		if (index >= 0) {
			cleanupDebtRef.current.splice(index, 1);
		}
	};

	const attemptCleanup = (candidate: ProgramSession<P>): Promise<void> => {
		if (candidate.cleanupComplete) {
			return Promise.resolve();
		}
		if (candidate.cleanupPromise) {
			return candidate.cleanupPromise;
		}

		const attempt = (async () => {
			await candidate.openPromise.catch(() => undefined);
			detachListener(candidate);
			if (candidate.program && !candidate.keepOpenOnUnmount) {
				await candidate.program.close();
			}
			candidate.cleanupComplete = true;
			removeCleanupDebt(candidate);
		})();
		const tracked = attempt.catch((error: unknown) => {
			candidate.cleanupPromise = undefined;
			throw error;
		});
		candidate.cleanupPromise = tracked;
		return tracked;
	};

	const cleanupWithRetry = async (
		candidate: ProgramSession<P>,
		attempts = 2,
	) => {
		let lastError: unknown;
		for (let attempt = 0; attempt < attempts; attempt += 1) {
			try {
				await attemptCleanup(candidate);
				return;
			} catch (error) {
				lastError = error;
			}
		}
		throw lastError;
	};

	const drainCleanupDebt = async (current: ProgramSession<P>) => {
		for (const candidate of [...cleanupDebtRef.current]) {
			if (candidate !== current) {
				await cleanupWithRetry(candidate);
			}
		}
	};

	useEffect(() => {
		if (!peer || !addressOrOpen || !targetKey) {
			setView(undefined);
			setResolvedId(undefined);
			programLoadingRef.current = undefined;
			return;
		}

		const request: ProgramRequest<P> = {
			peer,
			target: addressOrOpen,
			targetKey,
			id: options?.id,
			keepOpenOnUnmount,
		};
		const generation = ++generationRef.current;
		const candidate: ProgramSession<P> = {
			generation,
			request,
			active: true,
			keepOpenOnUnmount: request.keepOpenOnUnmount,
			openPromise: undefined as unknown as Promise<P | undefined>,
			cleanupComplete: false,
		};
		currentSessionRef.current = candidate;
		setView({ request, loading: true, peers: [] });
		setResolvedId(undefined);

		const predecessor = lifecycleTailRef.current;
		const openPromise = (async (): Promise<P | undefined> => {
			await predecessor;
			if (!candidate.active) {
				return undefined;
			}

			try {
				await drainCleanupDebt(candidate);
			} catch (error) {
				if (isCurrentSession(candidate)) {
					const resolvedError =
						error instanceof Error ? error : new Error(String(error));
					console.error(
						"failed to clean up the previous program",
						resolvedError,
					);
					setView({
						request,
						loading: false,
						error: resolvedError,
						peers: [],
					});
					forceUpdate();
				}
				return undefined;
			}
			if (!candidate.active) {
				return undefined;
			}

			let program: P;
			try {
				program = await peer.open(addressOrOpen as P | string, {
					...options,
					existing: "reuse",
				});
				candidate.program = program;
			} catch (error) {
				if (isCurrentSession(candidate)) {
					const resolvedError =
						error instanceof Error ? error : new Error(String(error));
					console.error("failed to open", resolvedError);
					setView({
						request,
						loading: false,
						error: resolvedError,
						peers: [],
					});
					forceUpdate();
				}
				return undefined;
			}

			if (!isCurrentSession(candidate)) {
				return program;
			}

			const subPrograms = (() => {
				try {
					const nested = (program as any)?.allPrograms;
					if (Array.isArray(nested)) return nested;
					if (
						nested &&
						typeof nested === "object" &&
						typeof (nested as any)[Symbol.iterator] === "function"
					) {
						return [...nested];
					}
				} catch {}
				return [];
			})();

			const hasTopics = [program, ...subPrograms].some((nested: any) => {
				try {
					return (
						nested?.closed === false &&
						typeof nested?.getTopics === "function" &&
						nested.getTopics().length > 0
					);
				} catch {
					return false;
				}
			});

			let peers: PublicSignKey[] = [];
			if (!hasTopics || typeof (program as any)?.getReady !== "function") {
				peers = [peer.identity.publicKey];
			} else {
				const updatePeers = () => {
					Promise.resolve()
						.then(() => program.getReady())
						.then((ready: Map<string, PublicSignKey>) => {
							if (!isCurrentSession(candidate)) return;
							setView((current) =>
								isSameRequest(current?.request, request)
									? { ...current!, peers: [...ready.values()] }
									: current,
							);
						})
						.catch((error: unknown) => {
							if (isCurrentSession(candidate)) {
								console.log("Error getReady()", error);
							}
						});
				};
				const changeListener = () => updatePeers();
				const events = program.events;
				let joinAttached = false;
				let leaveAttached = false;
				candidate.unsubscribe = () => {
					if (joinAttached) {
						events.removeEventListener("join", changeListener);
						joinAttached = false;
					}
					if (leaveAttached) {
						events.removeEventListener("leave", changeListener);
						leaveAttached = false;
					}
				};
				events.addEventListener("join", changeListener);
				joinAttached = true;
				events.addEventListener("leave", changeListener);
				leaveAttached = true;
				updatePeers();
			}

			if (!isCurrentSession(candidate)) {
				detachListener(candidate);
				return program;
			}

			setView({ request, program, loading: false, peers });
			forceUpdate();
			if (request.id) {
				let value: string | undefined = request.id;
				try {
					value = program.address;
				} catch {}
				setResolvedId({ request, value });
			}
			return program;
		})().catch((error: unknown) => {
			detachListener(candidate);
			if (isCurrentSession(candidate)) {
				const resolvedError =
					error instanceof Error ? error : new Error(String(error));
				console.error("failed to initialize the opened program", resolvedError);
				setView({
					request,
					loading: false,
					error: resolvedError,
					peers: [],
				});
				forceUpdate();
			}
			return undefined;
		});
		candidate.openPromise = openPromise;
		programLoadingRef.current = {
			request,
			owner: candidate,
			promise: openPromise,
		};

		return () => {
			candidate.active = false;
			detachListener(candidate);
			if (currentSessionRef.current === candidate) {
				currentSessionRef.current = undefined;
			}
			if (programLoadingRef.current?.owner === candidate) {
				programLoadingRef.current = undefined;
			}
			if (
				!candidate.keepOpenOnUnmount &&
				!cleanupDebtRef.current.includes(candidate)
			) {
				cleanupDebtRef.current.push(candidate);
			}

			const cleanup = lifecycleTailRef.current
				.catch(() => undefined)
				.then(() => cleanupWithRetry(candidate));
			lifecycleTailRef.current = cleanup.catch((error: unknown) => {
				console.error("failed to close program", error);
			});
		};
	}, [peer, targetKey, options?.id, keepOpenOnUnmount]);

	const currentView = isSameRequest(view?.request, currentRequest)
		? view
		: undefined;
	const currentLoading = isSameRequest(
		programLoadingRef.current?.request,
		currentRequest,
	)
		? programLoadingRef.current
		: undefined;
	const currentId = isSameRequest(resolvedId?.request, currentRequest)
		? resolvedId?.value
		: options?.id;
	const program = currentView?.program;
	const loading = currentView?.loading ?? false;
	const error = currentView?.error;
	const status: UseProgramStatus = !currentRequest
		? "idle"
		: loading
			? "loading"
			: error
				? "error"
				: program
					? "ready"
					: "idle";

	return {
		program,
		session,
		loading,
		status,
		error,
		promise: currentLoading?.promise,
		peers: currentView?.peers ?? [],
		id: currentId,
	};
}
