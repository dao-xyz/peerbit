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
// sits within the five lines above the declaration, and removing one
// requires shrinking the baseline (the intended direction).
import { readFileSync } from "node:fs";
import path from "node:path";

const TARGET = "packages/programs/data/shared-log/src/index.ts";
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

// The fence inventory as of the 2026-08-03 refactor baseline. Shrink this
// list as the session/lifecycle migration deletes fields; never grow it
// without a design note on the new field.
const BASELINE = new Set([
	"_checkedPruneRemoveBlocksLocalRangeMutationAdmission",
	"_joinWarmupGenerationByTarget",
	"_localReplicationRoleGeneration",
	"_nativeCoordinateMutationGenerations",
	"_receiveCleanupGateByPeer",
	"_receiveOwnershipMutationAdmissions",
	"_receiveOwnershipRevision",
	"_repairLifecycleController",
	"_repairSweepJoinWarmupGenerationByTarget",
	"_replicationAnnouncementRepairGeneration",
	"_replicationAnnouncementRepairGenerationController",
	"_replicationAnnouncementRetryGeneration",
	"_replicationInfoReceiveEpochByPeer",
	"_replicationLifecycleController",
	"_subscriptionEpochByPeer",
	"_subscriptionOpeningEpochByPeer",
]);

const root = process.cwd();
const lines = readFileSync(path.join(root, TARGET), "utf8").split("\n");

const found = new Map();
for (let i = 0; i < lines.length; i++) {
	const m = lines[i].match(DECL);
	const name = m?.[1] ?? m?.[2];
	if (!name || !matchesFenceTokens(name)) continue;
	// Accept a design-note: anywhere in the contiguous comment block directly
	// above the declaration (or within 5 lines when there is no block).
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

const errors = [];
for (const [name, info] of found) {
	if (!BASELINE.has(name) && !info.hasDesignNote) {
		errors.push(
			`NEW fence-pattern field "${name}" (${TARGET}:${info.line}). ` +
				`Prefer extending the session/lifecycle identity objects over adding ` +
				`another fence; if a new fence is genuinely required, put a ` +
				`\`design-note:\` marker in the comment block directly above the ` +
				`declaration explaining why identity does not cover this staleness, ` +
				`and add the field to BASELINE in this script.`,
		);
	}
}
for (const name of BASELINE) {
	if (!found.has(name)) {
		errors.push(
			`BASELINE entry "${name}" no longer exists in ${TARGET} — shrink ` +
				`BASELINE in scripts/ci/check-fence-ratchet.mjs (this is the good ` +
				`direction; the list must stay exact).`,
		);
	}
}

if (errors.length > 0) {
	for (const e of errors) console.error(e);
	process.exit(1);
}
console.log(
	`OK: ${found.size} fence-pattern fields in shared-log, all in the ratchet baseline (${BASELINE.size} entries).`,
);
