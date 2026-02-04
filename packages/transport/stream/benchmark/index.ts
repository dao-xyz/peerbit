/**
 * Benchmark entrypoint (wired to `pnpm -C packages/transport/stream run bench`).
 *
 * Examples:
 *   pnpm -C packages/transport/stream run bench -- directstream-sim --nodes 500 --degree 6
 *   pnpm -C packages/transport/stream run bench -- topology-sim --nodes 2000 --degree 4
 *   pnpm -C packages/transport/stream run bench -- transfer
 */

const argv = process.argv.slice(2);
const bench = argv[0] === "--" ? argv[1] : argv[0];

const usage = () => {
	console.log(
		[
			"@peerbit/stream benchmarks",
			"",
			"Usage:",
			"  pnpm -C packages/transport/stream run bench -- <benchmark> [args]",
			"",
			"Benchmarks:",
			"  directstream-sim  in-memory DirectStream network sim",
			"  topology-sim      discrete-event topology/routing sim",
			"  transfer          small real-libp2p throughput microbench",
			"",
			"Examples:",
			"  pnpm -C packages/transport/stream run bench -- directstream-sim --nodes 500 --degree 6",
			"  pnpm -C packages/transport/stream run bench -- topology-sim --nodes 2000 --degree 4",
			"  pnpm -C packages/transport/stream run bench -- transfer",
		].join("\n"),
	);
};

if (!bench || bench === "--help" || bench === "-h") {
	usage();
	process.exit(1);
}

switch (bench) {
	case "directstream-sim":
		await import("./directstream-sim.js");
		break;
	case "topology-sim":
		await import("./topology-sim.js");
		break;
	case "transfer":
		await import("./transfer.js");
		break;
	default:
		usage();
		process.exit(1);
}
