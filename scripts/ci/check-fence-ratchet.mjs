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
//
// 2026-08-12 — CLOSED NO-GO: the "stage 5 fence collapse" (folding the
// remaining ratcheted fields into session/lifecycle identity) was censused
// after the B12 legacy retirement landed and is not going to happen. Do not
// redo the census. Every remaining baseline field is structurally permanent
// for one of exactly two reasons:
//   1. It is a concurrency-DEPTH counter (a refcount of in-flight lanes),
//      not a staleness token. Identity answers "is this continuation from
//      the generation that started it?"; a depth counter answers "how many
//      lanes are open right now?" — a question identity cannot express.
//   2. Its lifetime is deliberately per-PEER, not per-session. The token is
//      set under one session and read (or cleared) under a LATER one, so
//      rotating identity would reset exactly the state that must survive the
//      rotation.
// Each such declaration carries a one-line permanence marker at its site;
// keep those markers with the fields if they ever move again.
import { readFileSync, readdirSync, statSync } from "node:fs";
import path from "node:path";

// Per-file frozen baselines. Fence fields drained out of index.ts into
// modules stay ratcheted in their new homes; TARGETS grows when an
// extraction creates a module holding fence-pattern fields.
const TARGETS = new Map([
	["packages/programs/data/shared-log/src/index.ts", ["_instanceLifecycle"]],
	[
		"packages/programs/data/shared-log/src/coordinate-persistence.ts",
		// The index.ts _nativeCoordinateMutationGenerations entry moved
		// file-to-file (stage 4.5: the coordinate persistence coordinator owns
		// its state), not fence growth.
		["_nativeCoordinateMutationGenerations"],
	],
	[
		"packages/programs/data/shared-log/src/checked-prune.ts",
		// peerRemovalFences predates this file's TARGETS entry; it entered the
		// baseline as existing inventory when the admission counter relocated
		// here, not as fence growth.
		[
			"_checkedPruneRemoveBlocksLocalRangeMutationAdmission",
			"peerRemovalFences",
		],
	],
	[
		"packages/programs/data/shared-log/src/instance-lifecycle.ts",
		// membershipLifecycleController/ownershipLifecycleController are the
		// index.ts _replicationLifecycleController/_repairLifecycleController
		// entries moved file-to-file (stage 4: the lifecycle owns its
		// controllers), not fence growth.
		[
			"_receiveOwnershipMutationAdmissions",
			"_receiveOwnershipRevision",
			"membershipLifecycleController",
			"ownershipLifecycleController",
			"roleGeneration",
		],
	],
	[
		"packages/programs/data/shared-log/src/native-write-through-block-store.ts",
		["nativeBlockWriteGenerations", "nativeDeleteEpoch"],
	],
	[
		"packages/programs/data/shared-log/src/peer-session.ts",
		["_receiveCleanupGateByPeer", "_replicationInfoReceiveEpochByPeer"],
	],
	[
		"packages/programs/data/shared-log/src/replication-info-v2-send.ts",
		// _senderEpoch predates this file's TARGETS entry; it entered the
		// baseline as existing inventory when the completeness leg below started
		// enumerating every src file, not as fence growth.
		["_senderEpoch"],
	],
	[
		"packages/programs/data/shared-log/src/replication-info-v2-receive.ts",
		// _reservedAdmissionsByPeer predates this file's TARGETS entry; it
		// entered the baseline as existing inventory when the completeness leg
		// below started enumerating every src file, not as fence growth.
		["_reservedAdmissionsByPeer"],
	],
	[
		"packages/programs/data/shared-log/src/sync/simple.ts",
		// These three predate this file's TARGETS entry; they entered the
		// baseline as existing inventory when the completeness leg below started
		// enumerating every src file, not as fence growth.
		[
			"syncDispatchLifecycleController",
			"syncDispatchTargetEpochCounter",
			"syncDispatchTargetEpochs",
		],
	],
	[
		"packages/programs/data/shared-log/src/sync/rateless-iblt.ts",
		// Both predate this file's TARGETS entry; they entered the baseline as
		// existing inventory when the completeness leg below started enumerating
		// every src file, not as fence growth.
		[
			"incomingRatelessProcessAdmissions",
			"ratelessDispatchLifecycleController",
		],
	],
	["packages/programs/data/shared-log/src/join-warmup.ts", []],
	["packages/programs/data/shared-log/src/replication-announcement.ts", []],
	["packages/programs/data/shared-log/src/replicator-liveness.ts", []],
	["packages/programs/data/shared-log/src/sync/factory.ts", []],
	// Stage 4: the pending-sync record store moved the simple synchronizer's
	// admission/claim state file-to-file. Its fields are deliberately plain
	// (no visibility modifier, no _-prefix), so any future fence-pattern
	// declaration matching DECL here is new growth and needs a design-note.
	["packages/programs/data/shared-log/src/sync/pending-sync-store.ts", []],
	// Stage 4: the shared dispatch-lifecycle registry moved the simple and
	// rateless synchronizers' duplicated lifecycle mechanics file-to-file.
	["packages/programs/data/shared-log/src/sync/dispatch-lifecycle.ts", []],
	["packages/programs/data/shared-log/src/sync/sync-peer-state.ts", []],
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

// Fence-pattern declarations in one file, keyed by field name.
const scanFile = (target) => {
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
	return found;
};

for (const [target, baselineList] of TARGETS) {
	const baseline = new Set(baselineList);
	totalBaseline += baseline.size;
	const found = scanFile(target);
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

// COMPLETENESS leg. The header claims this script freezes THE inventory, so a
// hand-maintained TARGETS list is not enough: a fence-pattern field added in a
// file nobody listed would be invisible. Enumerate every src file and require
// that each one is either a TARGETS key or genuinely fence-free.
//
// Intended workflow: a new file with a fence joins TARGETS in the same commit,
// or the declaration carries a `design-note:` marker. Declarations carrying
// that marker are exempt here too, so the two escape hatches cannot contradict
// each other (the per-file leg above already accepts a design-note in place of
// a baseline entry).
const SRC_ROOT = "packages/programs/data/shared-log/src";
const walkTs = (dir) =>
	readdirSync(path.join(root, dir))
		.sort()
		.flatMap((entry) => {
			const rel = `${dir}/${entry}`;
			if (statSync(path.join(root, rel)).isDirectory()) return walkTs(rel);
			return rel.endsWith(".ts") ? [rel] : [];
		});

for (const file of walkTs(SRC_ROOT)) {
	if (TARGETS.has(file)) continue;
	const unlisted = [...scanFile(file)].filter(
		([, info]) => !info.hasDesignNote,
	);
	if (unlisted.length === 0) continue;
	errors.push(
		`UNLISTED file with fence-pattern fields: ${file}. This script freezes ` +
			`the whole shared-log/src inventory, so the file must be a TARGETS key ` +
			`in scripts/ci/check-fence-ratchet.mjs (or every declaration must ` +
			`carry a \`design-note:\` marker). Baseline these declarations:\n` +
			unlisted
				.map(([name, info]) => `    "${name}", // ${file}:${info.line}`)
				.join("\n"),
	);
}

if (errors.length > 0) {
	for (const e of errors) console.error(e);
	process.exit(1);
}
console.log(
	`OK: ${totalFound} fence-pattern fields across ${TARGETS.size} shared-log files, all in the ratchet baselines (${totalBaseline} entries).`,
);
