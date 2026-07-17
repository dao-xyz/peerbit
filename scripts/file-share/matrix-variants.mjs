import { randomUUID } from "node:crypto";
import fsp from "node:fs/promises";
import path from "node:path";

const LABEL_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._-]*$/;
const UUID_PATTERN =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export const MATRIX_SESSION_MARKER = ".peerbit-file-share-matrix-session.json";

const isInside = (parent, candidate) => {
	const relative = path.relative(parent, candidate);
	return (
		relative !== "" && !relative.startsWith("..") && !path.isAbsolute(relative)
	);
};

export const createExclusiveMatrixSession = async ({
	baseDir,
	repoRoot,
	nonce = randomUUID(),
	now = new Date(),
}) => {
	if (!UUID_PATTERN.test(nonce)) {
		throw new Error("Matrix session nonce must be a UUID");
	}
	const requestedBase = path.resolve(baseDir);
	if (requestedBase === path.parse(requestedBase).root) {
		throw new Error("Refusing to use a filesystem root as the matrix base");
	}
	const requestedRepository = path.resolve(repoRoot);
	if (
		requestedBase === requestedRepository ||
		isInside(requestedRepository, requestedBase)
	) {
		throw new Error(
			"Refusing to create matrix output inside the Peerbit worktree",
		);
	}
	await fsp.mkdir(requestedBase, { recursive: true });
	const [matrixBase, repository] = await Promise.all([
		fsp.realpath(requestedBase),
		fsp.realpath(repoRoot),
	]);
	if (matrixBase === repository || isInside(repository, matrixBase)) {
		throw new Error(
			"Refusing to create matrix output inside the Peerbit worktree",
		);
	}
	const timestamp = now.toISOString().replaceAll(":", "-").replaceAll(".", "-");
	const matrixRoot = path.join(
		matrixBase,
		`peerbit-file-share-matrix-${timestamp}-${nonce}`,
	);
	await fsp.mkdir(matrixRoot, { recursive: false });
	const markerFile = path.join(matrixRoot, MATRIX_SESSION_MARKER);
	await fsp.writeFile(
		markerFile,
		`${JSON.stringify(
			{
				schema: {
					id: "peerbit-file-share-matrix-session",
					version: 1,
				},
				nonce,
				createdAt: now.toISOString(),
				matrixRoot,
			},
			null,
			2,
		)}\n`,
		{ flag: "wx" },
	);
	return { matrixBase, matrixRoot, nonce, markerFile };
};

const parseToken = (token) => {
	if (token === "current") {
		return { name: "current", kind: "worktree" };
	}
	if (token === "head") {
		return { name: "head", kind: "ref", ref: "HEAD" };
	}
	const separator = token.indexOf("=");
	if (separator < 1 || separator === token.length - 1) {
		const downstreamHint =
			token === "downstream"
				? ' Use an explicit real ref such as "downstream=origin/my-branch".'
				: "";
		throw new Error(
			`Invalid matrix variant "${token}". Expected current, head, or name=git-ref.${downstreamHint}`,
		);
	}
	const name = token.slice(0, separator).trim();
	const ref = token.slice(separator + 1).trim();
	if (!LABEL_PATTERN.test(name)) {
		throw new Error(`Invalid matrix variant label "${name}"`);
	}
	if (ref.length === 0 || ref.startsWith("-") || /\s/.test(ref)) {
		throw new Error(`Invalid git ref for matrix variant "${name}"`);
	}
	return { name, kind: "ref", ref };
};

export const assertMatrixIntegrationMode = (integrationMode) => {
	if (integrationMode !== "link") {
		throw new Error(
			`A core ref matrix requires --integration-mode link; "${String(integrationMode)}" cannot preserve the selected Peerbit dependency graph`,
		);
	}
	return integrationMode;
};

export const assertMatrixPackageRequest = ({
	requestedNames,
	requiredNames,
}) => {
	if (requestedNames == null) {
		return;
	}
	if (
		!Array.isArray(requestedNames) ||
		requestedNames.some(
			(name) => typeof name !== "string" || name.length === 0,
		) ||
		new Set(requestedNames).size !== requestedNames.length
	) {
		throw new Error(
			"--local-packages must contain unique, non-empty package names",
		);
	}
	const missingRequiredPackages = requiredNames.filter(
		(name) => !requestedNames.includes(name),
	);
	if (missingRequiredPackages.length > 0) {
		throw new Error(
			`Matrix package selection is missing required file-share packages: ${missingRequiredPackages.join(", ")}`,
		);
	}
};

export const normalizeVariantSpecs = (value) => {
	const tokens = (value ?? "baseline=origin/master")
		.split(",")
		.map((item) => item.trim())
		.filter(Boolean);
	if (tokens.length === 0) {
		throw new Error("At least one matrix variant is required");
	}
	const specs = tokens.map(parseToken);
	const names = new Set();
	for (const spec of specs) {
		if (names.has(spec.name)) {
			throw new Error(`Duplicate matrix variant label "${spec.name}"`);
		}
		names.add(spec.name);
	}
	return specs;
};

export const assertUniqueResolvedVariantCommits = (specs) => {
	const commits = new Map();
	for (const spec of specs) {
		if (!/^[0-9a-f]{40}$/.test(spec.resolvedCommit ?? "")) {
			throw new Error(`Variant "${spec.name}" has no resolved git commit`);
		}
		const existing = commits.get(spec.resolvedCommit);
		if (existing) {
			throw new Error(
				`Matrix variants "${existing}" and "${spec.name}" both resolve to ${spec.resolvedCommit}`,
			);
		}
		commits.set(spec.resolvedCommit, spec.name);
	}
	return specs;
};

export const createVariantMaterializationPlan = ({
	variantSpec,
	variantRoot,
}) => {
	if (!variantSpec || !["worktree", "ref"].includes(variantSpec.kind)) {
		throw new Error(
			"Variant materialization requires a worktree or ref variant",
		);
	}
	if (!/^[0-9a-f]{40}$/.test(variantSpec.resolvedCommit ?? "")) {
		throw new Error("Variant materialization requires a resolved git commit");
	}
	if (typeof variantRoot !== "string" || variantRoot.length === 0) {
		throw new Error("Variant materialization requires an isolated destination");
	}
	if (variantSpec.kind === "ref" && !variantSpec.ref) {
		throw new Error("Ref variant materialization requires its requested ref");
	}
	return {
		cloneCommit: variantSpec.resolvedCommit,
		peerbitRoot: path.resolve(variantRoot),
		requestedRef:
			variantSpec.kind === "worktree" ? "worktree" : variantSpec.ref,
		sourceWorktreeMustBeClean: variantSpec.kind === "worktree",
	};
};

export const createCounterbalancedInvocationPlan = ({
	variants,
	modes,
	runs,
}) => {
	if (!Array.isArray(variants) || variants.length === 0) {
		throw new Error("Counterbalanced plan requires at least one variant");
	}
	if (!Array.isArray(modes) || modes.length === 0) {
		throw new Error("Counterbalanced plan requires at least one mode");
	}
	if (!Number.isSafeInteger(runs) || runs <= 0) {
		throw new Error("Counterbalanced plan requires a positive run count");
	}
	const plan = [];
	let sequence = 0;
	for (let run = 1; run <= runs; run++) {
		const round = run - 1;
		const offset = round % variants.length;
		let variantOrder = [
			...variants.slice(offset),
			...variants.slice(0, offset),
		];
		if (Math.floor(round / variants.length) % 2 === 1) {
			variantOrder = variantOrder.toReversed();
		}
		for (let lap = 0; lap < modes.length; lap++) {
			for (let position = 0; position < variantOrder.length; position++) {
				const modeIndex = (lap + position + round) % modes.length;
				plan.push({
					sequence: ++sequence,
					run,
					variant: variantOrder[position],
					mode: modes[modeIndex],
				});
			}
		}
	}
	return plan;
};

export const createCounterbalancedModePlan = ({ modes, runs }) =>
	createCounterbalancedInvocationPlan({
		variants: ["standalone"],
		modes,
		runs,
	}).map(({ sequence, run, mode }) => ({ sequence, run, mode }));
