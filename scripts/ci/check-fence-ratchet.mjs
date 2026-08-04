// Ratchet against fence-per-symptom growth in shared-log (invoked from the
// Lint step, next to check-shard-coverage.mjs).
//
// Between June and July 2026, packages/programs/data/shared-log/src/index.ts
// grew 8k -> 28k lines and accumulated ~30 fence mechanisms (epoch /
// generation / revision / gate / admission fields), each patching one
// instance of the same bug class: async continuations outliving the state
// they started under. The session/lifecycle refactor is draining these into
// identity-checked session objects. This script freezes the inventory:
// adding a NEW fence-pattern field fails CI unless a `design-note:` comment
// in the comment block above the declaration justifies it, and removing one
// requires shrinking the baseline (the intended direction).
import { readFileSync } from "node:fs";
import path from "node:path";

// Per-file frozen baselines. Fence fields drained out of index.ts into
// modules stay ratcheted in their new homes; TARGETS grows when an
// extraction creates a module holding fence-pattern fields.
const TARGETS = new Map([
	[
		"packages/programs/data/shared-log/src/index.ts",
		[
			"_checkedPruneRemoveBlocksLocalRangeMutationAdmission",
			"_instanceLifecycle",
			"_nativeCoordinateMutationGenerations",
			"_receiveCleanupGateByPeer",
			"_receiveOwnershipMutationAdmissions",
			"_receiveOwnershipRevision",
			"_repairLifecycleController",
			"_replicationInfoReceiveEpochByPeer",
			"_replicationLifecycleController",
			"_subscriptionOpeningEpochByPeer",
		],
	],
	[
		"packages/programs/data/shared-log/src/instance-lifecycle.ts",
		["roleGeneration"],
	],
	[
		"packages/programs/data/shared-log/src/native-write-through-block-store.ts",
		["nativeBlockWriteGenerations", "nativeDeleteEpoch"],
	],
	[
		"packages/programs/data/shared-log/src/peer-session.ts",
		["_subscriptionEpochByPeer"],
	],
	[
		"packages/programs/data/shared-log/src/join-warmup.ts",
		[
			"_joinWarmupGenerationByTarget",
			"_repairSweepJoinWarmupGenerationByTarget",
		],
	],
	[
		"packages/programs/data/shared-log/src/replication-announcement.ts",
		[
			"_replicationAnnouncementRepairGeneration",
			"_replicationAnnouncementRepairGenerationController",
			"_replicationAnnouncementRetryGeneration",
		],
	],
	["packages/programs/data/shared-log/src/replicator-liveness.ts", []],
	["packages/programs/data/shared-log/src/sync/factory.ts", []],
]);
// Token match is per camelCase/underscore segment so "generationOfLastPrune"
// and "epochCounter" are caught while "aggregateTotals" is not.
const TOKEN_SET = new Set([
	"epoch",
	"epochs",
	"generation",
	"generations",
	"revision",
	"revisions",
	"fence",
	"fences",
	"gate",
	"gates",
	"watermark",
	"admission",
	"admissions",
	"lifecycle",
]);
const matchesFenceTokens = (name) =>
	name
		.split(/(?=[A-Z])|_|#/)
		.some((segment) => TOKEN_SET.has(segment.toLowerCase()));
// Class fields with a visibility modifier, plus modifier-less/static fields
// whose names use the file's _-prefix (or #private) field idiom. Bare
// "name: Type;" type-literal members and local const/let bindings stay
// excluded.
const DECL =
	/^\s*(?:(?:private|protected|public)\s+(?:static\s+)?(?:override\s+)?(?:readonly\s+)?([A-Za-z0-9_#]+)|(?:static\s+)?(?:readonly\s+)?([_#][A-Za-z0-9_]+))\s*[?!]?\s*[:=]/;


const root = process.cwd();
const errors = [];
let totalFound = 0;
let totalBaseline = 0;

for (const [target, baselineList] of TARGETS) {
	const baseline = new Set(baselineList);
	totalBaseline += baseline.size;
	const lines = readFileSync(path.join(root, target), "utf8").split("\n");

	const found = new Map();
	for (let i = 0; i < lines.length; i++) {
		const m = lines[i].match(DECL);
		const name = m?.[1] ?? m?.[2];
		if (!name || !matchesFenceTokens(name)) continue;
		// Accept a design-note: anywhere in the contiguous comment block
		// directly above the declaration (or within 5 lines when there is no
		// block).
		let start = i - 1;
		while (
			start >= 0 &&
			/^\s*(\/\/|\/\*|\*)/.test(lines[start]) &&
			i - start < 40
		) {
			start--;
		}
		const hasDesignNote = lines
			.slice(Math.max(0, Math.min(start + 1, i - 5)), i)
			.some((l) => l.includes("design-note:"));
		found.set(name, { line: i + 1, hasDesignNote });
	}
	totalFound += found.size;

	for (const [name, info] of found) {
		if (!baseline.has(name) && !info.hasDesignNote) {
			errors.push(
				`NEW fence-pattern field "${name}" (${target}:${info.line}). ` +
					`Prefer extending the session/lifecycle identity objects over ` +
					`adding another fence; if a new fence is genuinely required, put ` +
					`a \`design-note:\` marker in the comment block directly above ` +
					`the declaration explaining why identity does not cover this ` +
					`staleness, and add the field to this file's baseline in ` +
					`scripts/ci/check-fence-ratchet.mjs.`,
			);
		}
	}
	for (const name of baseline) {
		if (!found.has(name)) {
			errors.push(
				`Baseline entry "${name}" no longer exists in ${target} — shrink ` +
					`that file's baseline in scripts/ci/check-fence-ratchet.mjs ` +
					`(this is the good direction; the lists must stay exact).`,
			);
		}
	}
}

if (errors.length > 0) {
	for (const e of errors) console.error(e);
	process.exit(1);
}
console.log(
	`OK: ${totalFound} fence-pattern fields across ${TARGETS.size} shared-log files, all in the ratchet baselines (${totalBaseline} entries).`,
);
