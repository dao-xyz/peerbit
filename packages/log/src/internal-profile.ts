export type InternalProfileValue = string | number | boolean | undefined;
export type InternalProfileEvent = {
	name: string;
	component?: string;
	durationMs?: number;
	entries?: number;
	bytes?: number;
	messages?: number;
	count?: number;
	details?: Record<string, InternalProfileValue>;
};
export type InternalProfileSink = (event: InternalProfileEvent) => void;

export const internalProfileNow = () =>
	globalThis.performance?.now?.() ?? Date.now();
export const internalProfileStart = (sink: InternalProfileSink | undefined) =>
	sink ? internalProfileNow() : 0;
export const emitInternalProfileDuration = (
	sink: InternalProfileSink | undefined,
	startedAt: number,
	event: Omit<InternalProfileEvent, "durationMs">,
) => {
	if (!sink) return;
	sink({ ...event, durationMs: internalProfileNow() - startedAt });
};

// Carry an already-isolated sink between internal callers without adding an
// option/getter to EntryV0.create's public input or retaining invocation state.
const profiles = new WeakMap<object, InternalProfileSink>();
export const withInternalProfile = <T extends object>(
	value: T,
	profile: InternalProfileSink | undefined,
): T => {
	if (profile) profiles.set(value, profile);
	return value;
};
export const getInternalProfile = (value: object) => profiles.get(value);
