export async function withPeerShutdown(peers, run) {
	let bodyFailed = false;
	let bodyFailure;
	let stopFailures;
	try {
		await run();
	} catch (error) {
		bodyFailed = true;
		bodyFailure = error;
	} finally {
		const results = await Promise.allSettled(
			peers.map(async (peer) => peer.stop()),
		);
		stopFailures = results.flatMap((result) =>
			result.status === "rejected" ? [result.reason] : [],
		);
	}
	const failures = [...(bodyFailed ? [bodyFailure] : []), ...stopFailures];
	if (failures.length === 1) throw failures[0];
	if (failures.length > 1) {
		throw new AggregateError(failures, "Replication and peer shutdown failed");
	}
}
