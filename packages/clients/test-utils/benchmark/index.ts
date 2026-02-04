/**
 * Benchmark entrypoint (wired to `pnpm -C packages/clients/test-utils run bench`).
 */

const usage = () => {
	console.log(
		[
			"@peerbit/test-utils benchmarks",
			"",
			"Usage:",
			"  pnpm -C packages/clients/test-utils run bench -- <benchmark> [args]",
			"",
			"Benchmarks:",
			"  fanout-peerbit-sim  FanoutTree sim using full Peerbit clients over in-memory libp2p",
			"",
			"Examples:",
			"  pnpm -C packages/clients/test-utils run bench -- fanout-peerbit-sim --preset ci-small",
			"  pnpm -C packages/clients/test-utils run bench -- fanout-peerbit-sim --preset scale-1k --nodes 1000 --seed 1",
			"  pnpm -C packages/clients/test-utils run bench -- fanout-peerbit-sim --nodes 1000 --degree 6 --messages 300 --msgSize 1024 --msgRate 30 --seed 1",
			"  pnpm -C packages/clients/test-utils run bench -- fanout-peerbit-sim --nodes 3 --degree 2 --subscribers 2 --messages 5 --msgSize 32 --intervalMs 0 --seed 1 --timeoutMs 300000",
		].join("\n"),
	);
};

const main = async () => {
	const argv = process.argv.slice(2);
	const bench = argv[0] === "--" ? argv[1] : argv[0];

	if (!bench || bench === "--help" || bench === "-h") {
		usage();
		process.exit(1);
	}

	switch (bench) {
		case "fanout-peerbit-sim":
			await import("./fanout-peerbit-sim.js");
			break;
		default:
			usage();
			process.exit(1);
	}
};

void main().catch((err) => {
	console.error(err);
	process.exitCode = 1;
});
