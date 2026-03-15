import {
	defaultExamplesDest,
	defaultExamplesSource,
	parseArgs,
	prepareExamplesRepo,
	repoRoot,
} from "./common.mjs";

const usage = () => {
	console.log(`Prepare a disposable peerbit-examples checkout pinned to the local peerbit workspace.

Usage:
  node ./scripts/file-share/prepare-peerbit-examples.mjs [options]

Options:
  --source <path-or-url>     examples repo source (default: ${defaultExamplesSource()})
  --dest <path>              output directory (default: ${defaultExamplesDest()})
  --peerbit-root <path>      peerbit workspace root (default: ${repoRoot})
  --integration-mode <mode>  one of none, link, overlay (default: overlay)
  --local-packages <csv>     package names to link/overlay (default: @peerbit/shared-log)
  --fresh                    delete the destination before cloning
  --install                  run pnpm install in the prepared checkout
`);
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	if (args.help) {
		usage();
		return;
	}
	const integrationMode = args["integration-mode"] ?? "overlay";
	const localPackages =
		integrationMode === "none"
			? []
			: (args["local-packages"] ?? "@peerbit/shared-log")
					.split(",")
					.map((value) => value.trim())
					.filter(Boolean);

	const prepared = await prepareExamplesRepo({
		source: args.source,
		dest: args.dest,
		peerbitRoot: args["peerbit-root"] ?? repoRoot,
		fresh: Boolean(args.fresh),
		install: Boolean(args.install),
		localPackageNames: localPackages,
		applyOverrides: integrationMode === "link",
	});

	console.log(`Prepared examples checkout: ${prepared.dest}`);
	console.log(`Local peerbit workspace: ${prepared.peerbitRoot}`);
	console.log(`Integration mode: ${integrationMode}`);
	console.log(`Configured local packages: ${prepared.localPackages.size}`);
	console.log("");
	console.log("Next steps:");
	console.log(`  pnpm --dir ${prepared.dest} install`);
	console.log(
		`  pnpm --dir ${prepared.dest}/packages/file-share/frontend exec playwright test tests/uploadChurn.local.manual.e2e.spec.ts -c playwright.config.ts`,
	);
};

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
