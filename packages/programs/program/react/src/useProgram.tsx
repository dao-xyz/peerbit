import type { Identity } from "@peerbit/crypto";
import { PublicSignKey } from "@peerbit/crypto";
import type { OpenOptions, Program, ProgramEvents } from "@peerbit/program";
import { useEffect, useReducer, useRef, useState } from "react";

const addressOrDefined = <A, B extends ProgramEvents, P extends Program<A, B>>(
	p?: P,
) => {
	try {
		return p?.address;
	} catch (error) {
		return !!p;
	}
};
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
	const [program, setProgram] = useState<P | undefined>();
	const [id, setId] = useState<string | undefined>(options?.id);
	const [loading, setLoading] = useState(false);
	const [error, setError] = useState<Error | undefined>(undefined);
	const [session, forceUpdate] = useReducer((x) => x + 1, 0);
	const programLoadingRef = useRef<Promise<P | undefined> | undefined>(
		undefined,
	);
	const [peers, setPeers] = useState<PublicSignKey[]>([]);

	const closingRef = useRef<Promise<any>>(Promise.resolve());
	/*   if (options?.debug) {
          console.log("useProgram", addressOrOpen, options);
      } */
	useEffect(() => {
		if (!peer || !addressOrOpen) {
			setLoading(false);
			setError(undefined);
			setPeers([]);
			setProgram(undefined);
			programLoadingRef.current = undefined;
			return;
		}
		setError(undefined);
		setLoading(true);
		let changeListener: (() => void) | undefined = undefined;

		closingRef.current.then(() => {
			programLoadingRef.current = peer
				?.open(addressOrOpen as P | string, {
					...options,
					existing: "reuse",
				})
				.then((p: P) => {
					const subPrograms = (() => {
						try {
							const candidate = (p as any)?.allPrograms;
							if (Array.isArray(candidate)) return candidate;
							if (
								candidate &&
								typeof candidate === "object" &&
								typeof (candidate as any)[Symbol.iterator] === "function"
							) {
								return [...candidate];
							}
						} catch {}
						return [];
					})();

					const hasTopics =
						[p, ...subPrograms].filter(
							(x: any) =>
								x?.closed === false &&
								typeof x?.getTopics === "function" &&
								x.getTopics?.().length > 0,
						).length > 0;

					// If program has no topics (or isn't a full Program instance), we can
					// still use it; just default to "self" as the only known peer.
					if (!hasTopics || typeof (p as any)?.getReady !== "function") {
						setPeers([peer.identity.publicKey]);
					} else {
						changeListener = () => {
							p.getReady().then((ready: Map<string, PublicSignKey>) => {
								setPeers([...ready.values()]);
							});
						};
						p.events.addEventListener("join", changeListener);
						p.events.addEventListener("leave", changeListener);
						p.getReady()
							.then((ready: Map<string, PublicSignKey>) => {
								setPeers([...ready.values()]);
							})
							.catch((e: any) => {
								console.log("Error getReady()", e);
							});
					}

					setProgram(p);
					forceUpdate();
					if (options?.id) {
						setId(p.address);
					}
					return p;
				})
				.catch((e: unknown) => {
					const err = e instanceof Error ? e : new Error(String(e));
					console.error("failed to open", err);
					setProgram(undefined);
					setPeers([]);
					setError(err);
					forceUpdate();
					return undefined;
				})
				.finally(() => {
					setLoading(false);
				});
		});

		// TODO AbortController?
		return () => {
			let startRef = programLoadingRef.current;

			// TODO don't close on reopen the same db?
			if (programLoadingRef.current) {
				closingRef.current =
					programLoadingRef.current.then((p) => {
						if (!p) return;
						const unsubscribe = () => {
							changeListener &&
								p.events.removeEventListener("join", changeListener);
							changeListener &&
								p.events.removeEventListener("leave", changeListener);
						};

						if (programLoadingRef.current === startRef) {
							setProgram(undefined);
							programLoadingRef.current = undefined as any;
						}

						if (options?.keepOpenOnUnmount) {
							unsubscribe();
							return; // nothing to close
						}

						return p.close().then(unsubscribe);
					}) || Promise.resolve();
			}
		};
	}, [
		peer?.identity.publicKey.hashcode(),
		options?.id,
		typeof addressOrOpen === "string"
			? addressOrOpen
			: addressOrDefined(addressOrOpen as P),
	]);
	const status: UseProgramStatus =
		!peer || !addressOrOpen
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
		promise: programLoadingRef.current,
		peers,
		id,
	};
}
