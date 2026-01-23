export type SeekRoutingMode =
	| { kind: "seek"; redundancy: number }
	| { kind: "non-seek" };

export type ShouldIgnoreDataMessageArgs = {
	signedBySelf: boolean;
	seenBefore: number;
	mode: SeekRoutingMode;
};

export const shouldIgnoreDataMessage = (args: ShouldIgnoreDataMessageArgs) => {
	if (args.signedBySelf) {
		return true;
	}

	if (args.mode.kind === "seek") {
		return args.seenBefore >= args.mode.redundancy;
	}

	return args.seenBefore > 0;
};

export type SelectSeekRelayTargetsArgs<T, Id extends string | number> = {
	candidates: Iterable<T>;
	getCandidateId: (candidate: T) => Id;
	inboundId: Id;
	hasSigned: (candidateId: Id) => boolean;
};

export const selectSeekRelayTargets = <T, Id extends string | number>(
	args: SelectSeekRelayTargetsArgs<T, Id>,
): T[] => {
	const out: T[] = [];
	for (const candidate of args.candidates) {
		const candidateId = args.getCandidateId(candidate);
		if (candidateId === args.inboundId) continue;
		if (args.hasSigned(candidateId)) continue;
		out.push(candidate);
	}
	return out;
};

export type ShouldAcknowledgeDataMessageArgs = {
	isRecipient: boolean;
	seenBefore: number;
	redundancy: number;
};

export const shouldAcknowledgeDataMessage = (
	args: ShouldAcknowledgeDataMessageArgs,
) => {
	return args.isRecipient && args.seenBefore < args.redundancy;
};

export type SeekAckRouteUpdate<Id extends string | number> = {
	from: Id;
	neighbour: Id;
	target: Id;
	distance: number;
};

export const computeSeekAckRouteUpdate = <Id extends string | number>(args: {
	current: Id;
	upstream?: Id;
	downstream: Id;
	target: Id;
	distance: number;
}): SeekAckRouteUpdate<Id> => {
	return {
		from: args.upstream ?? args.current,
		neighbour: args.downstream,
		target: args.target,
		distance: args.distance,
	};
};
