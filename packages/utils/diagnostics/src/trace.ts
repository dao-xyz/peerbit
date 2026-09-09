import {
	type DiagnosticEvent,
	type DiagnosticSink,
	type DiagnosticValue,
	diagnosticNow,
} from "./index.js";

let invocation = 0;
const DETAIL_EVENT_LIMIT = 256;

/** A bounded, invocation-local diagnostic stream; never a durability proof. */
export const createDiagnosticTrace = (
	sink: DiagnosticSink | undefined,
	component: string,
	prefix: string,
):
	| undefined
	| {
			id: string;
			now: typeof diagnosticNow;
			emit(event: DiagnosticEvent): void;
			finish(event: DiagnosticEvent): void;
	  } => {
	if (!sink) return;
	invocation = (invocation % Number.MAX_SAFE_INTEGER) + 1;
	const id = `${prefix}:${invocation}`;
	const startedAt = diagnosticNow();
	let detailCount = 0;
	let droppedEvents = 0;

	const snapshot = (event: DiagnosticEvent): DiagnosticEvent => {
		const details: Record<string, DiagnosticValue> = {};
		for (const [key, value] of Object.entries(event.details ?? {})) {
			if (
				value === undefined ||
				typeof value === "string" ||
				typeof value === "number" ||
				typeof value === "boolean"
			) {
				details[key] = value;
			}
		}
		return {
			...event,
			component: event.component ?? component,
			traceId: id,
			details: {
				...details,
				v: 1,
				elapsedMs: Math.max(0, diagnosticNow() - startedAt),
			},
		};
	};
	const deliver = (target: DiagnosticSink, event: DiagnosticEvent) => {
		try {
			// A caller can supply an async function despite the void sink type.
			// Own its rejection without making diagnostics part of operation latency.
			const result: unknown = target(event);
			if (
				result &&
				typeof (result as PromiseLike<unknown>).then === "function"
			) {
				void Promise.resolve(result).catch(() => {});
			}
		} catch {
			// Diagnostic failures must not change the observed operation's outcome.
		}
	};

	return {
		id,
		now: diagnosticNow,
		emit(event) {
			if (!sink) return;
			if (detailCount === DETAIL_EVENT_LIMIT) {
				droppedEvents = Math.min(Number.MAX_SAFE_INTEGER, droppedEvents + 1);
				return;
			}
			detailCount++;
			deliver(sink, snapshot(event));
		},
		finish(event) {
			if (!sink) return;
			const terminalSink = sink;
			sink = undefined;
			const terminal = snapshot(event);
			terminal.details = {
				...terminal.details,
				emittedEvents: detailCount + 1,
				droppedEvents,
			};
			deliver(terminalSink, terminal);
		},
	};
};
