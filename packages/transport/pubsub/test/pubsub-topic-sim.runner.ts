import { ready as cryptoReady } from "@peerbit/crypto";
import {
	resolvePubsubTopicSimParams,
	runPubsubTopicSim,
} from "../benchmark/pubsub-topic-sim-lib.js";

const main = async () => {
	const raw = process.argv[2];
	if (!raw) {
		throw new Error("Missing PubsubTopicSim params JSON");
	}

	await cryptoReady;
	const params = resolvePubsubTopicSimParams(JSON.parse(raw));
	const result = await runPubsubTopicSim(params);
	process.stdout.write(JSON.stringify(result));
};

try {
	await main();
} catch (error: any) {
	console.error(error?.stack ?? error?.message ?? String(error));
	process.exit(1);
}
