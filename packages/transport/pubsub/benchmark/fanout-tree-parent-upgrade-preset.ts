export type UpgradeMode = "direct" | "probe" | "shadow";
export type UpgradePreset = "raw" | "default-candidate";
export type EvidenceHarness = "single" | "multi";

/**
 * Benchmark/CI-only preset wiring for fanout parent-upgrade evidence.
 *
 * Do not import this module from runtime code. The production default remains
 * `parentUpgradeIntervalMs: 0`; this helper only keeps evaluator, prepush, and
 * default-readiness runner defaults reproducible.
 */
export type ParentUpgradePresetConfig = {
	parentUpgradePreset: UpgradePreset;
	parentUpgradeIntervalMs: number;
	parentUpgradeLeafOnly: boolean;
	parentUpgradeMinLevelGain: number;
	parentUpgradeRootMinLevelGain: number;
	parentUpgradeRootMinSubtreeGain: number;
	parentUpgradeNonRootMinLevelGain: number;
	parentUpgradeMinFreeSlots: number;
	parentUpgradeRootMinFreeSlots: number;
	parentUpgradeMaxChildLoadRatio: number;
	parentUpgradeRootMaxChildLoadRatio: number;
	parentUpgradeCooldownMs: number;
	parentUpgradeFailedBackoffMinMs: number;
	parentUpgradeFailedBackoffMaxMs: number;
	parentUpgradeQuietMs: number;
	parentUpgradeRepairQuietMs: number;
	parentUpgradeMaxPerPeer: number;
	parentUpgradeRepairGuard: boolean;
	parentUpgradeDataGuard: boolean;
	parentUpgradeMode: UpgradeMode;
	parentUpgradeVerifyStaleRootCapacity: boolean;
	parentUpgradeStaleRootProbeProbability: number;
	parentProbeTimeoutMs: number;
	parentProbeMaxPerRound: number;
	parentProbeMaxLagMessages: number;
	parentProbeRejectCooldownMs: number;
	parentProbeRejectCooldownMaxMs: number;
	parentShadowObserveMs: number;
	parentShadowMinObservations: number;
	parentShadowDualPathMs: number;
	parentShadowDualPathMinMessages: number;
};

export type ParentUpgradeRuntimeOptions = Omit<
	ParentUpgradePresetConfig,
	"parentUpgradePreset"
>;

export type EvidenceFailure = {
	metric: string;
	baseline: number;
	upgrade: number;
	limit: number;
};

const PARENT_UPGRADE_RUNTIME_OPTION_KEYS = [
	"parentUpgradeIntervalMs",
	"parentUpgradeLeafOnly",
	"parentUpgradeMinLevelGain",
	"parentUpgradeRootMinLevelGain",
	"parentUpgradeRootMinSubtreeGain",
	"parentUpgradeNonRootMinLevelGain",
	"parentUpgradeMinFreeSlots",
	"parentUpgradeRootMinFreeSlots",
	"parentUpgradeMaxChildLoadRatio",
	"parentUpgradeRootMaxChildLoadRatio",
	"parentUpgradeCooldownMs",
	"parentUpgradeFailedBackoffMinMs",
	"parentUpgradeFailedBackoffMaxMs",
	"parentUpgradeQuietMs",
	"parentUpgradeRepairQuietMs",
	"parentUpgradeMaxPerPeer",
	"parentUpgradeRepairGuard",
	"parentUpgradeDataGuard",
	"parentUpgradeMode",
	"parentUpgradeVerifyStaleRootCapacity",
	"parentUpgradeStaleRootProbeProbability",
	"parentProbeTimeoutMs",
	"parentProbeMaxPerRound",
	"parentProbeMaxLagMessages",
	"parentProbeRejectCooldownMs",
	"parentProbeRejectCooldownMaxMs",
	"parentShadowObserveMs",
	"parentShadowMinObservations",
	"parentShadowDualPathMs",
	"parentShadowDualPathMinMessages",
] as const satisfies readonly (keyof ParentUpgradeRuntimeOptions)[];

export const DEFAULT_CANDIDATE_PRESET = "default-candidate" as const;
export const DEFAULT_PARENT_UPGRADE_SEEDS = [1, 2, 3] as const;
export const DEFAULT_PARENT_UPGRADE_SEED_CSV =
	DEFAULT_PARENT_UPGRADE_SEEDS.join(",");
export const DEFAULT_PARENT_UPGRADE_FAST_SEED_CSV = "1";
export const PARENT_UPGRADE_FRONTIER_ROOT_CAPS = [
	"0.2",
	"0.225",
	"0.25",
	"0.4",
] as const;

export const defaultCandidateArgs = (...args: string[]) => [
	"--parentUpgradePreset",
	DEFAULT_CANDIDATE_PRESET,
	...args,
];

export const parentUpgradeRuntimeOptions = (
	config: ParentUpgradeRuntimeOptions,
	enabled = true,
): ParentUpgradeRuntimeOptions =>
	Object.fromEntries(
		PARENT_UPGRADE_RUNTIME_OPTION_KEYS.map((key) => [
			key,
			key === "parentUpgradeIntervalMs" && !enabled ? 0 : config[key],
		]),
	) as ParentUpgradeRuntimeOptions;

export const parseBool01 = (value: string | undefined, fallback: boolean) => {
	if (value === undefined) return fallback;
	return value === "1";
};

export const parseCsvNumbers = (
	value: string | undefined,
	fallback: readonly number[],
) => {
	if (!value) return [...fallback];
	const parsed = value
		.split(",")
		.map((part) => Number(part.trim()))
		.filter((part) => Number.isFinite(part));
	return parsed.length > 0 ? parsed : [...fallback];
};

export const ratioLimit = (
	baseline: number,
	ratio: number,
	absoluteSlack = 1e-9,
) => Math.max(absoluteSlack, baseline * ratio + absoluteSlack);

export const avgFinite = (values: number[]) => {
	const finite = values.filter((value) => Number.isFinite(value));
	return finite.length === 0
		? NaN
		: finite.reduce((sum, value) => sum + value, 0) / finite.length;
};

export const maxFinite = (values: number[]) => {
	const finite = values.filter((value) => Number.isFinite(value));
	return finite.length === 0 ? NaN : Math.max(...finite);
};

export const fmt = (value: number, digits = 2) =>
	Number.isFinite(value) ? value.toFixed(digits) : "NaN";

export const parseParentUpgradePreset = (
	value: string | undefined,
): UpgradePreset => {
	const preset = value ?? "raw";
	if (preset !== "raw" && preset !== DEFAULT_CANDIDATE_PRESET) {
		throw new Error(`Unknown parent upgrade preset: ${preset}`);
	}
	return preset;
};

export const isDefaultCandidatePreset = (preset: UpgradePreset) =>
	preset === DEFAULT_CANDIDATE_PRESET;

export const defaultEvidenceLimitsForPreset = (
	preset: UpgradePreset,
	harness: EvidenceHarness,
) => {
	const defaultCandidate = isDefaultCandidatePreset(preset);
	return {
		maxProbePerUpgrade: defaultCandidate ? (harness === "multi" ? 8 : 5) : 2,
		maxRootChildrenDelta: defaultCandidate ? 2 : 4,
		maxRootUploadPctDelta: 1,
	};
};

export const parseParentUpgradePresetConfig = (
	get: (name: string) => string | undefined,
): ParentUpgradePresetConfig => {
	const parentUpgradePreset = parseParentUpgradePreset(
		get("--parentUpgradePreset"),
	);
	const defaultCandidate = isDefaultCandidatePreset(parentUpgradePreset);
	const parentUpgradeQuietMs = Number(get("--parentUpgradeQuietMs") ?? 5_000);
	const parentUpgradeMaxChildLoadRatio = Number(
		get("--parentUpgradeMaxChildLoadRatio") ?? 0.5,
	);
	const parentUpgradeRootMinLevelGain = Number(
		get("--parentUpgradeRootMinLevelGain") ?? 3,
	);
	const parentUpgradeMinFreeSlots = Number(
		get("--parentUpgradeMinFreeSlots") ?? 8,
	);
	const parentUpgradeRootMaxChildLoadRatio = Number(
		get("--parentUpgradeRootMaxChildLoadRatio") ??
			Math.min(parentUpgradeMaxChildLoadRatio, 0.4),
	);
	const parentUpgradeModeRaw = get("--parentUpgradeMode");
	const parentUpgradeMode =
		parentUpgradeModeRaw === "probe" || parentUpgradeModeRaw === "shadow"
			? parentUpgradeModeRaw
			: parentUpgradeModeRaw === "direct"
				? "direct"
				: defaultCandidate
					? "shadow"
					: "direct";

	return {
		parentUpgradePreset,
		parentUpgradeIntervalMs: Number(get("--parentUpgradeIntervalMs") ?? 1_000),
		parentUpgradeLeafOnly: parseBool01(get("--parentUpgradeLeafOnly"), true),
		parentUpgradeMinLevelGain: Number(get("--parentUpgradeMinLevelGain") ?? 2),
		parentUpgradeRootMinLevelGain,
		parentUpgradeRootMinSubtreeGain: Number(
			get("--parentUpgradeRootMinSubtreeGain") ?? parentUpgradeRootMinLevelGain,
		),
		parentUpgradeNonRootMinLevelGain: Number(
			get("--parentUpgradeNonRootMinLevelGain") ?? 2,
		),
		parentUpgradeMinFreeSlots,
		parentUpgradeRootMinFreeSlots: Number(
			get("--parentUpgradeRootMinFreeSlots") ?? parentUpgradeMinFreeSlots,
		),
		parentUpgradeMaxChildLoadRatio,
		parentUpgradeRootMaxChildLoadRatio,
		parentUpgradeCooldownMs: Number(get("--parentUpgradeCooldownMs") ?? 5_000),
		parentUpgradeFailedBackoffMinMs: Number(
			get("--parentUpgradeFailedBackoffMinMs") ?? 5_000,
		),
		parentUpgradeFailedBackoffMaxMs: Number(
			get("--parentUpgradeFailedBackoffMaxMs") ?? 60_000,
		),
		parentUpgradeQuietMs,
		parentUpgradeRepairQuietMs: Number(
			get("--parentUpgradeRepairQuietMs") ?? parentUpgradeQuietMs,
		),
		parentUpgradeMaxPerPeer: Number(get("--parentUpgradeMaxPerPeer") ?? 2),
		parentUpgradeRepairGuard: parseBool01(
			get("--parentUpgradeRepairGuard"),
			true,
		),
		parentUpgradeDataGuard: parseBool01(get("--parentUpgradeDataGuard"), true),
		parentUpgradeMode,
		parentUpgradeVerifyStaleRootCapacity: parseBool01(
			get("--parentUpgradeVerifyStaleRootCapacity"),
			defaultCandidate,
		),
		parentUpgradeStaleRootProbeProbability: Number(
			get("--parentUpgradeStaleRootProbeProbability") ?? 0.015625,
		),
		parentProbeTimeoutMs: Number(get("--parentProbeTimeoutMs") ?? 500),
		parentProbeMaxPerRound: Number(get("--parentProbeMaxPerRound") ?? 2),
		parentProbeMaxLagMessages: Number(get("--parentProbeMaxLagMessages") ?? 0),
		parentProbeRejectCooldownMs: Number(
			get("--parentProbeRejectCooldownMs") ?? 10_000,
		),
		parentProbeRejectCooldownMaxMs: Number(
			get("--parentProbeRejectCooldownMaxMs") ?? 60_000,
		),
		parentShadowObserveMs: Number(get("--parentShadowObserveMs") ?? 2_000),
		parentShadowMinObservations: Number(
			get("--parentShadowMinObservations") ?? 2,
		),
		parentShadowDualPathMs: Number(
			get("--parentShadowDualPathMs") ?? (defaultCandidate ? 5_000 : 0),
		),
		parentShadowDualPathMinMessages: Number(
			get("--parentShadowDualPathMinMessages") ?? (defaultCandidate ? 32 : 1),
		),
	};
};
