import path from "node:path";
import {
	buildPeerbitPackages,
	cleanPeerbitBuildArtifacts,
	defaultExamplesDest,
	defaultExamplesSource,
	defaultFileShareLocalPackages,
	ensureExamplesAssetPackageLinks,
	getFileShareConsumerRoots,
	installPinnedExamplesDependencies,
	parseArgs,
	prepareExamplesRepo,
	repoRoot,
	run,
} from "./common.mjs";
import { instrumentFileShareViteConfigs } from "./vite-instrumentation.mjs";

const usage = () => {
	console.log(`Prepare a disposable peerbit-examples checkout pinned to the local peerbit workspace.

Usage:
  node ./scripts/file-share/prepare-peerbit-examples.mjs [options]

Options:
  --source <path-or-url>     examples repo source (default: ${defaultExamplesSource()})
  --dest <path>              output directory (default: ${defaultExamplesDest()})
  --peerbit-root <path>      peerbit workspace root (default: ${repoRoot})
  --integration-mode <mode>  one of none, link (default: link)
  --local-packages <csv>     package names to link (default: ${defaultFileShareLocalPackages.join(",")}; use "all" for every installed local package)
  --fresh                    delete the destination before cloning
`);
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	if (args.help) {
		usage();
		return;
	}
	const integrationMode = args["integration-mode"] ?? "link";
	if (!["none", "link"].includes(integrationMode)) {
		throw new Error(
			`Unsupported --integration-mode "${integrationMode}". Expected "none" or "link".`,
		);
	}
	const localPackages =
		integrationMode === "none"
			? []
			: args["local-packages"] === "all"
				? undefined
				: (args["local-packages"] ?? defaultFileShareLocalPackages.join(","))
						.split(",")
						.map((value) => value.trim())
						.filter(Boolean);

	const prepared = await prepareExamplesRepo({
		source: args.source,
		dest: args.dest,
		peerbitRoot: path.resolve(args["peerbit-root"] ?? repoRoot),
		fresh: Boolean(args.fresh),
		install: false,
		localPackageNames: localPackages,
		applyOverrides: false,
	});
	const effectiveLocalPackageNames = [...prepared.localPackages.keys()];
	if (integrationMode === "link") {
		run("pnpm", ["install", "--frozen-lockfile"], {
			cwd: prepared.peerbitRoot,
		});
		await cleanPeerbitBuildArtifacts({
			peerbitRoot: prepared.peerbitRoot,
			packageNames: effectiveLocalPackageNames,
		});
		buildPeerbitPackages(prepared.peerbitRoot, effectiveLocalPackageNames);
	}
	await installPinnedExamplesDependencies(prepared.dest);
	if (integrationMode === "link") {
		await ensureExamplesAssetPackageLinks({
			examplesRoot: prepared.dest,
			peerbitRoot: prepared.peerbitRoot,
			packageNames: effectiveLocalPackageNames,
			consumerRoots: getFileShareConsumerRoots(prepared.dest),
		});
		await instrumentFileShareViteConfigs(
			getFileShareConsumerRoots(prepared.dest)[0],
		);
	}

	console.log(`Prepared examples checkout: ${prepared.dest}`);
	console.log(`Local peerbit workspace: ${prepared.peerbitRoot}`);
	console.log(`Integration mode: ${integrationMode}`);
	console.log(
		`Configured local packages: ${effectiveLocalPackageNames.length}`,
	);
	console.log("");
	console.log("Next steps:");
	console.log(
		`  pnpm --dir ${prepared.dest}/packages/file-share/frontend exec playwright test tests/uploadChurn.local.manual.e2e.spec.ts -c playwright.config.ts`,
	);
};

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
