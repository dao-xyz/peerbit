export {
	diagnosticStart as syncProfileStart,
	emitDiagnosticDuration as emitSyncProfileDuration,
	emitDiagnosticEvent as emitSyncProfileEvent,
} from "@peerbit/diagnostics";
export type {
	DiagnosticEvent as SyncProfileEvent,
	DiagnosticSink as SyncProfileFn,
} from "@peerbit/diagnostics";
