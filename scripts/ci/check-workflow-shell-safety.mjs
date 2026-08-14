// Refuses inline single-quoted interpreter scripts in workflow `run:` blocks.
//
// WHY: `node --input-type=module -e '<script>'` ends at the first apostrophe in
// the script -- including one inside a comment. The rest of the "script" is then
// handed to the shell as commands. That is not a syntax error, so nothing warns:
//
//   run 31844749062 died with
//     /home/runner/work/_temp/....sh: line 57: //: Is a directory
//     Process completed with exit code 126
//
//   because a comment in the embedded JS read "that is the reporter's job".
//
// `bash -n` does NOT catch this -- verified against the broken revision, which
// parses cleanly. The damage is semantic, not syntactic, so the only reliable
// guard is structural: do not use the fragile form at all.
//
// The fix is a quoted heredoc, which benchmarks.yml already uses:
//
//   node --input-type=module <<'NODE'
//   ...script, apostrophes and all...
//   NODE
//
// 'NODE' is quoted so the shell performs no expansion inside, and a heredoc is
// not a pipe, so the exit status still belongs to node. The terminator must sit
// at the `run:` block's base indentation, because YAML dedents the block scalar
// and a heredoc terminator has to start at column 0 of the resulting script.
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";

const WORKFLOW_DIR = ".github/workflows";

// `cmd -e '` at the end of a line: the script body starts on the next line and
// runs until a lone apostrophe. Covers node/python/ruby/perl alike.
const INLINE_SCRIPT = /\s-e\s+'\s*$/;

export const findInlineSingleQuotedScripts = (source, file) => {
	const findings = [];
	source.split("\n").forEach((line, index) => {
		if (INLINE_SCRIPT.test(line))
			findings.push({
				file,
				line: index + 1,
				text: line.trim(),
			});
	});
	return findings;
};

export const scanWorkflows = (root = process.cwd()) => {
	const dir = path.join(root, WORKFLOW_DIR);
	const files = readdirSync(dir).filter(
		(name) => name.endsWith(".yml") || name.endsWith(".yaml"),
	);
	if (files.length === 0)
		throw new Error(
			`${WORKFLOW_DIR} contains no workflow files; this guard would pass by scanning nothing`,
		);
	return files.flatMap((name) =>
		findInlineSingleQuotedScripts(
			readFileSync(path.join(dir, name), "utf8"),
			path.join(WORKFLOW_DIR, name),
		),
	);
};

const main = () => {
	const findings = scanWorkflows();
	if (findings.length === 0) {
		console.log(
			"check-workflow-shell-safety: no inline single-quoted scripts in workflow run: blocks",
		);
		return;
	}
	console.error(
		"check-workflow-shell-safety: inline single-quoted interpreter scripts found.\n" +
			"A single apostrophe anywhere in the script -- including inside a comment --\n" +
			"terminates the shell string, and the remainder is executed as commands.\n" +
			"bash -n does not catch it. Use a quoted heredoc instead:\n" +
			"\n" +
			"    node --input-type=module <<'NODE'\n" +
			"    ...\n" +
			"    NODE\n" +
			"\n" +
			"with the terminator at the run: block's base indentation.\n",
	);
	for (const finding of findings)
		console.error(`  ${finding.file}:${finding.line}  ${finding.text}`);
	process.exitCode = 1;
};

if (process.argv[1] && import.meta.url.endsWith(path.basename(process.argv[1])))
	main();
