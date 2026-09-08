import { type DiagnosticValue, diagnosticNow } from "@peerbit/diagnostics";
import type { SyncProfileFn } from "./profile.js";

// Process-local correlation only, never a receipt, wire id or persistent key.
let nextTrace = 0;
const MAX_EVENTS = 256;
export const PERSISTED_DELIVERY_PROFILE_ENTRY_LIMIT = 16;

/** Bounded, invocation-owned diagnostics. No retained entries or peer objects. */
export const createPersistedDeliveryProfile = (
	sink: SyncProfileFn | undefined,
	entries: number,
	minAcks: number,
	leaderDegree: number,
) => {
	if (!sink) return undefined;
	nextTrace = (nextTrace % Number.MAX_SAFE_INTEGER) + 1;
	const traceId = `persisted:${nextTrace}`;
	const startedAt = diagnosticNow();
	let emitted = 0;
	let dropped = 0;
	let closed = false;
	const send = (
		name: string,
		details: Record<string, DiagnosticValue>,
		peer?: string,
		phaseStartedAt = startedAt,
	) => {
		try {
			const now = diagnosticNow();
			const result: unknown = sink({
				name: `sharedLog.persistedDelivery.${name}`,
				component: "shared-log",
				traceId,
				entries,
				peer,
				durationMs: Math.max(0, now - phaseStartedAt),
				details: {
					...details,
					v: 1,
					minAcks,
					leaderDegree,
					entrySampleWindow: Math.min(
						entries,
						PERSISTED_DELIVERY_PROFILE_ENTRY_LIMIT,
					),
					entriesOutsideSampleWindow: Math.max(
						0,
						entries - PERSISTED_DELIVERY_PROFILE_ENTRY_LIMIT,
					),
					elapsedMs: Math.max(0, now - startedAt),
				},
			});
			// A void callback can still be async in TypeScript. Never await it, and
			// own its rejection just as we isolate a synchronous diagnostic throw.
			if (
				result &&
				typeof (result as PromiseLike<unknown>).then === "function"
			) {
				void Promise.resolve(result).catch(() => undefined);
			}
		} catch {
			// Diagnostics must not enter protocol recovery or replace its outcome.
		}
	};
	const emit = (
		name: string,
		details: Record<string, DiagnosticValue>,
		peer?: string,
		phaseStartedAt?: number,
	) => {
		if (closed) return;
		if (emitted >= MAX_EVENTS) {
			dropped = Math.min(Number.MAX_SAFE_INTEGER, dropped + 1);
			return;
		}
		emitted++;
		send(name, details, peer, phaseStartedAt);
	};
	return {
		now: diagnosticNow,
		emit,
		phase: (
			phase: string,
			options: {
				round: number;
				peer: string;
				requestedEntries: number;
				attempt?: number;
				timeoutMs?: number;
				startedAt?: number;
			},
		) => {
			const { peer, startedAt = diagnosticNow(), ...details } = options;
			emit(
				"peerPhase",
				{ ...details, phase, edge: "start", outcome: "pending" },
				peer,
				startedAt,
			);
			return (outcome: string, acceptedEntries?: number) =>
				emit(
					"peerPhase",
					{ ...details, phase, edge: "end", outcome, acceptedEntries },
					peer,
					startedAt,
				);
		},
		finish: (details: Record<string, DiagnosticValue>) => {
			if (closed) return;
			closed = true;
			send("settle", {
				...details,
				emittedEvents: emitted + 1,
				droppedEvents: dropped,
			});
		},
	};
};
