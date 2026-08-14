import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";
import {
	findInlineSingleQuotedScripts,
	scanWorkflows,
} from "./check-workflow-shell-safety.mjs";

test("flags the exact form that broke run 31844749062", () => {
	// Reconstructed from the revision that failed, indentation and all.
	const broken = [
		"        run: |",
		"          assert_suite_json() {",
		'            SUITE_JSON_FILE="$1" node --input-type=module -e \'',
		"              // that is the reporter's job, and",
		"              const parsed = JSON.parse(raw);",
		"            '",
		"          }",
	].join("\n");
	const findings = findInlineSingleQuotedScripts(broken, "x.yml");
	assert.equal(findings.length, 1);
	assert.equal(findings[0].line, 3);
});

test("accepts the quoted-heredoc form, apostrophes and all", () => {
	const fixed = [
		"        run: |",
		"          node --input-type=module <<'NODE'",
		"          // that is the reporter's job, and it is fine here",
		'          console.log("don\'t panic");',
		"          NODE",
	].join("\n");
	assert.deepEqual(findInlineSingleQuotedScripts(fixed, "x.yml"), []);
});

test("does not flag an -e flag that is not opening a script block", () => {
	// `grep -e 'pattern'` and friends close on the same line; only a trailing
	// quote opens a multi-line body.
	const benign = [
		"        run: |",
		"          grep -e 'foo' file",
		"          node -e 'console.log(1)'",
	].join("\n");
	assert.deepEqual(findInlineSingleQuotedScripts(benign, "x.yml"), []);
});

test("the repository's own workflows are clean", () => {
	assert.deepEqual(
		scanWorkflows().map((finding) => `${finding.file}:${finding.line}`),
		[],
	);
});

test("the checker exits non-zero when a workflow regresses", () => {
	// Belt and braces: the module-level guard must actually fail the process,
	// not merely return findings nobody reads.
	const source = [
		"import { findInlineSingleQuotedScripts } from './scripts/ci/check-workflow-shell-safety.mjs';",
		"const broken = `run: |\\n  node -e '\\n  // it's here\\n  '`;",
		"if (findInlineSingleQuotedScripts(broken, 'x.yml').length !== 1) process.exit(2);",
	].join("\n");
	const out = execFileSync(
		process.execPath,
		["--input-type=module", "-e", source],
		{
			encoding: "utf8",
			stdio: ["ignore", "pipe", "pipe"],
		},
	);
	assert.equal(out, "");
});
