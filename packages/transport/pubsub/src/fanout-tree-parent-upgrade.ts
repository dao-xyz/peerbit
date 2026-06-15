export type ParentUpgradeOptions = {
	parentUpgradeIntervalMs?: number;
	parentUpgradeLeafOnly?: boolean;
	parentUpgradeMinLevelGain?: number;
	parentUpgradeRootMinLevelGain?: number;
	parentUpgradeRootMinSubtreeGain?: number;
	parentUpgradeNonRootMinLevelGain?: number;
	parentUpgradeMinFreeSlots?: number;
	parentUpgradeRootMinFreeSlots?: number;
	parentUpgradeMaxChildLoadRatio?: number;
	parentUpgradeRootMaxChildLoadRatio?: number;
	parentUpgradeCooldownMs?: number;
	parentUpgradeFailedBackoffMinMs?: number;
	parentUpgradeFailedBackoffMaxMs?: number;
	parentUpgradeQuietMs?: number;
	parentUpgradeRepairQuietMs?: number;
	parentUpgradeMaxPerPeer?: number;
	parentUpgradeRepairGuard?: boolean;
	parentUpgradeDataGuard?: boolean;
	parentUpgradeMode?: ParentUpgradeMode;
	parentUpgradeVerifyStaleRootCapacity?: boolean;
	parentUpgradeStaleRootProbeProbability?: number;
	parentProbeTimeoutMs?: number;
	parentProbeMaxPerRound?: number;
	parentProbeMaxLagMessages?: number;
	parentProbeRejectCooldownMs?: number;
	parentProbeRejectCooldownMaxMs?: number;
	parentShadowObserveMs?: number;
	parentShadowMinObservations?: number;
	parentShadowDualPathMs?: number;
	parentShadowDualPathMinMessages?: number;
};

export type ParentUpgradeSkipReason =
	| "leaf"
	| "repair"
	| "data"
	| "cooldown"
	| "quiet"
	| "budget";

export type ParentUpgradeMode = "direct" | "probe" | "shadow";

export type ParentUpgradePolicy = {
	intervalMs: number;
	leafOnly: boolean;
	minLevelGain: number;
	rootMinLevelGain: number;
	rootMinSubtreeGain: number;
	nonRootMinLevelGain: number;
	minFreeSlots: number;
	rootMinFreeSlots: number;
	maxChildLoadRatio: number;
	rootMaxChildLoadRatio: number;
	staleRootProbeProbability: number;
	cooldownMs: number;
	quietMs: number;
	repairQuietMs: number;
	maxPerPeer: number;
	repairGuard: boolean;
	dataGuard: boolean;
	mode: ParentUpgradeMode;
	verifyStaleRootCapacity: boolean;
	failedBackoff: {
		minMs: number;
		maxMs: number;
	};
	probe: {
		timeoutMs: number;
		maxPerRound: number;
		maxLagMessages: number;
		rejectCooldownMs: number;
		rejectCooldownMaxMs: number;
	};
	shadow: {
		observeMs: number;
		minObservations: number;
		dualPathMs: number;
		dualPathMinMessages: number;
	};
};

export type ParentUpgradeGateState = {
	children: { size: number };
	missingSeqs: { size: number };
	lastRepairSentAt: number;
	endSeqExclusive: number;
	parentUpgradeRetryAfterSeq: number;
	maxSeqSeen: number;
	parentUpgradeCount: number;
	parentUpgradeBackoffUntil: number;
	parentUpgradeLastAt: number;
	lastParentDataAt: number;
	lastParentUpgradeActivityAt?: number;
};

export type ParentUpgradeGateOptions = {
	leafOnly: boolean;
	repairGuard: boolean;
	dataGuard: boolean;
	endedAndComplete: boolean;
	maxPerPeer: number;
	cooldownMs: number;
	quietMs: number;
	repairQuietMs: number;
	now: number;
};

export type ParentUpgradeSkipMetrics = {
	reparentUpgradeSkipLeaf: number;
	reparentUpgradeSkipRepair: number;
	reparentUpgradeSkipData: number;
	reparentUpgradeSkipCooldown: number;
	reparentUpgradeSkipQuiet: number;
	reparentUpgradeSkipBudget: number;
};

export const normalizeParentUpgradePolicy = (
	options: ParentUpgradeOptions,
): ParentUpgradePolicy => {
	const intervalMs = Math.max(
		0,
		Math.floor(options.parentUpgradeIntervalMs ?? 0),
	);
	const minLevelGain = Math.max(
		1,
		Math.floor(options.parentUpgradeMinLevelGain ?? 1),
	);
	const rootMinLevelGain = Math.max(
		minLevelGain,
		Math.floor(options.parentUpgradeRootMinLevelGain ?? 3),
	);
	const rootMinSubtreeGain = Math.max(
		minLevelGain,
		Math.floor(options.parentUpgradeRootMinSubtreeGain ?? rootMinLevelGain),
	);
	const minFreeSlots = Math.max(
		0,
		Math.floor(options.parentUpgradeMinFreeSlots ?? 8),
	);
	const maxChildLoadRatioRaw = Number(
		options.parentUpgradeMaxChildLoadRatio ?? 0.5,
	);
	const maxChildLoadRatio = Number.isFinite(maxChildLoadRatioRaw)
		? Math.max(0, maxChildLoadRatioRaw)
		: 0.5;
	const rootMaxChildLoadRatioRaw = Number(
		options.parentUpgradeRootMaxChildLoadRatio ??
			Math.min(maxChildLoadRatio, 0.4),
	);
	const rootMaxChildLoadRatio = Number.isFinite(rootMaxChildLoadRatioRaw)
		? Math.max(0, rootMaxChildLoadRatioRaw)
		: Math.min(maxChildLoadRatio, 0.4);
	const staleRootProbeProbabilityRaw = Number(
		options.parentUpgradeStaleRootProbeProbability ?? 0.015625,
	);
	const staleRootProbeProbability = Number.isFinite(
		staleRootProbeProbabilityRaw,
	)
		? Math.max(0, Math.min(1, staleRootProbeProbabilityRaw))
		: 0.015625;
	const cooldownMs = Math.max(
		0,
		Math.floor(options.parentUpgradeCooldownMs ?? 5_000),
	);
	const failedBackoffMinMs = Math.max(
		0,
		Math.floor(options.parentUpgradeFailedBackoffMinMs ?? cooldownMs),
	);
	const probeRejectCooldownMs = Math.max(
		0,
		Math.floor(options.parentProbeRejectCooldownMs ?? 10_000),
	);
	const quietMs = Math.max(
		0,
		Math.floor(options.parentUpgradeQuietMs ?? 5_000),
	);
	const mode: ParentUpgradeMode =
		options.parentUpgradeMode === "probe" ||
		options.parentUpgradeMode === "shadow"
			? options.parentUpgradeMode
			: options.parentUpgradeMode === "direct"
				? "direct"
				: "shadow";

	return {
		intervalMs,
		leafOnly: options.parentUpgradeLeafOnly !== false,
		minLevelGain,
		rootMinLevelGain,
		rootMinSubtreeGain,
		nonRootMinLevelGain: Math.max(
			minLevelGain,
			Math.floor(options.parentUpgradeNonRootMinLevelGain ?? 2),
		),
		minFreeSlots,
		rootMinFreeSlots: Math.max(
			0,
			Math.floor(options.parentUpgradeRootMinFreeSlots ?? minFreeSlots),
		),
		maxChildLoadRatio,
		rootMaxChildLoadRatio,
		staleRootProbeProbability,
		cooldownMs,
		quietMs,
		repairQuietMs: Math.max(
			0,
			Math.floor(options.parentUpgradeRepairQuietMs ?? quietMs),
		),
		maxPerPeer: Math.max(
			0,
			Math.floor(options.parentUpgradeMaxPerPeer ?? 2),
		),
		repairGuard: options.parentUpgradeRepairGuard !== false,
		dataGuard: options.parentUpgradeDataGuard !== false,
		mode,
		verifyStaleRootCapacity:
			options.parentUpgradeVerifyStaleRootCapacity ?? (mode === "shadow"),
		failedBackoff: {
			minMs: failedBackoffMinMs,
			maxMs: Math.max(
				failedBackoffMinMs,
				Math.floor(options.parentUpgradeFailedBackoffMaxMs ?? 60_000),
			),
		},
		probe: {
			timeoutMs: Math.max(
				1,
				Math.floor(options.parentProbeTimeoutMs ?? 500),
			),
			maxPerRound: Math.max(
				1,
				Math.floor(options.parentProbeMaxPerRound ?? 2),
			),
			maxLagMessages: Math.max(
				0,
				Math.floor(options.parentProbeMaxLagMessages ?? 0),
			),
			rejectCooldownMs: probeRejectCooldownMs,
			rejectCooldownMaxMs: Math.max(
				probeRejectCooldownMs,
				Math.floor(options.parentProbeRejectCooldownMaxMs ?? 60_000),
			),
		},
		shadow: {
			observeMs: Math.max(
				0,
				Math.floor(options.parentShadowObserveMs ?? 2_000),
			),
			minObservations: Math.max(
				1,
				Math.floor(options.parentShadowMinObservations ?? 2),
			),
			dualPathMs: Math.max(
				0,
				Math.floor(
					options.parentShadowDualPathMs ?? (mode === "shadow" ? 5_000 : 0),
				),
			),
			dualPathMinMessages: Math.max(
				1,
				Math.floor(
					options.parentShadowDualPathMinMessages ??
						(mode === "shadow" ? 32 : 1),
				),
			),
		},
	};
};

export const evaluateParentUpgradeGate = (
	state: ParentUpgradeGateState,
	options: ParentUpgradeGateOptions,
): { run: true } | { run: false; reason: ParentUpgradeSkipReason } => {
	if (options.leafOnly && state.children.size > 0) {
		return { run: false, reason: "leaf" };
	}
	if (options.repairGuard && state.missingSeqs.size > 0) {
		return { run: false, reason: "repair" };
	}
	if (
		options.repairGuard &&
		options.repairQuietMs > 0 &&
		state.lastRepairSentAt > 0 &&
		options.now - state.lastRepairSentAt < options.repairQuietMs
	) {
		return { run: false, reason: "repair" };
	}
	if (
		options.dataGuard &&
		!(state.endSeqExclusive > 0 && options.endedAndComplete)
	) {
		return { run: false, reason: "data" };
	}
	if (options.dataGuard && state.parentUpgradeRetryAfterSeq >= 0) {
		if (state.maxSeqSeen <= state.parentUpgradeRetryAfterSeq) {
			return { run: false, reason: "data" };
		}
		state.parentUpgradeRetryAfterSeq = -1;
	}
	if (
		options.maxPerPeer > 0 &&
		state.parentUpgradeCount >= options.maxPerPeer
	) {
		return { run: false, reason: "budget" };
	}
	if (state.parentUpgradeBackoffUntil > options.now) {
		return { run: false, reason: "cooldown" };
	}
	if (
		options.cooldownMs > 0 &&
		state.parentUpgradeLastAt > 0 &&
		options.now - state.parentUpgradeLastAt < options.cooldownMs
	) {
		return { run: false, reason: "cooldown" };
	}
	const lastParentUpgradeActivityAt =
		state.lastParentUpgradeActivityAt ?? state.lastParentDataAt;
	if (
		options.quietMs > 0 &&
		lastParentUpgradeActivityAt > 0 &&
		options.now - lastParentUpgradeActivityAt < options.quietMs
	) {
		return { run: false, reason: "quiet" };
	}
	return { run: true };
};

export const recordParentUpgradeSkip = (
	metrics: ParentUpgradeSkipMetrics,
	reason: ParentUpgradeSkipReason,
) => {
	switch (reason) {
		case "leaf":
			metrics.reparentUpgradeSkipLeaf += 1;
			break;
		case "repair":
			metrics.reparentUpgradeSkipRepair += 1;
			break;
		case "data":
			metrics.reparentUpgradeSkipData += 1;
			break;
		case "cooldown":
			metrics.reparentUpgradeSkipCooldown += 1;
			break;
		case "quiet":
			metrics.reparentUpgradeSkipQuiet += 1;
			break;
		case "budget":
			metrics.reparentUpgradeSkipBudget += 1;
			break;
	}
};
