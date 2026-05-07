export type DiagnosticValue = string | number | boolean | undefined;

export type DiagnosticEvent = {
	/**
	 * Stable event or phase name, for example "shared-log.sync.queueSync".
	 */
	name: string;
	component?: string;
	durationMs?: number;
	count?: number;
	entries?: number;
	symbols?: number;
	messages?: number;
	bytes?: number;
	targets?: number;
	cacheHit?: boolean;
	peer?: string;
	traceId?: string;
	syncId?: string;
	details?: Record<string, DiagnosticValue>;
};

export type DiagnosticSink = (event: DiagnosticEvent) => void;

export const diagnosticNow = () =>
	globalThis.performance?.now?.() ?? Date.now();

export const diagnosticStart = (sink: DiagnosticSink | undefined) =>
	sink ? diagnosticNow() : 0;

export const emitDiagnosticEvent = (
	sink: DiagnosticSink | undefined,
	event: DiagnosticEvent,
) => {
	sink?.(event);
};

export const emitDiagnosticDuration = (
	sink: DiagnosticSink | undefined,
	startedAt: number,
	event: Omit<DiagnosticEvent, "durationMs">,
) => {
	if (!sink) {
		return;
	}
	sink({
		...event,
		durationMs: diagnosticNow() - startedAt,
	});
};
