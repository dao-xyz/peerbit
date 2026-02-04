/**
 * Benchmark entrypoint (wired to `pnpm -C packages/transport/pubsub run bench`).
 *
 * Examples:
 *   pnpm -C packages/transport/pubsub run bench -- topic-sim --nodes 2000 --degree 6
 */

const argv = process.argv.slice(2);
const bench = argv[0] === "--" ? argv[1] : argv[0];

const usage = () => {
	console.log(
		[
			"@peerbit/pubsub benchmarks",
			"",
			"Usage:",
			"  pnpm -C packages/transport/pubsub run bench -- <benchmark> [args]",
			"",
			"Benchmarks:",
			"  topic-sim  in-memory DirectSub topic fanout sim",
			"  tree-sim   capacity-aware tree overlay sim (Plumtree-inspired)",
			"  fanout-tree-sim  end-to-end FanoutTree protocol sim (bootstrap tracker join)",
			"",
			"Example:",
			"  pnpm -C packages/transport/pubsub run bench -- topic-sim --nodes 3 --degree 2 --subscribers 2 --messages 5 --msgSize 32 --intervalMs 0 --seed 1 --subscribeModel preseed --timeoutMs 300000",
			"  pnpm -C packages/transport/pubsub run bench -- topic-sim --nodes 2000 --degree 6 --subscribers 1500 --messages 200",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-sim --preset live --nodes 2000 --bootstraps 1 --seed 1",
		].join("\n"),
	);
};

if (!bench || bench === "--help" || bench === "-h") {
	usage();
	process.exit(1);
}

switch (bench) {
	case "topic-sim":
		await import("./pubsub-topic-sim.js");
		break;
	case "tree-sim":
		await import("./pubsub-tree-sim.js");
		break;
	case "fanout-tree-sim":
		await import("./fanout-tree-sim.js");
		break;
	default:
		usage();
		process.exit(1);
}
