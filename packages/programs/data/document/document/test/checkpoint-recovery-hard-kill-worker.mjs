import { createStore } from "@peerbit/any-store";
import { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import fs from "node:fs/promises";

// Plain Node executes the compiled fixture under test. This worker may itself
// have been copied into dist/test; neither layout depends on the caller's cwd.
let fixtureUrl = new URL("./utils/checkpoint-recovery.js", import.meta.url);
try {
	await fs.access(fixtureUrl);
} catch {
	fixtureUrl = new URL(
		"../dist/test/utils/checkpoint-recovery.js",
		import.meta.url,
	);
}
const {
	CheckpointRecoveryFixture,
	MAX_RECOVERY_RECORD_BYTES,
	decodeRecoveryInput,
} = await import(fixtureUrl.href);

const [mode, directory, trustPath, ingressPath, killPhase] =
	process.argv.slice(2);
if (
	(mode !== "write" && mode !== "read") ||
	!directory ||
	!trustPath ||
	(mode === "write" && (!ingressPath || !killPhase)) ||
	(mode === "read" && ingressPath)
) {
	throw new Error("Expected write/read, checkpoint directory and trust file");
}

const fromHex = (value) => {
	if (typeof value !== "string" || !/^(?:[0-9a-f]{2})*$/.test(value)) {
		throw new Error("Invalid hexadecimal fixture bytes");
	}
	return new Uint8Array(Buffer.from(value, "hex"));
};
const wireTrust = JSON.parse(await fs.readFile(trustPath, "utf8"));
const trust = { ...wireTrust, owner: fromHex(wireTrust.owner) };
// No reader ingress path exists: durable checkpoint evidence is the only input.
const input =
	mode === "write"
		? decodeRecoveryInput(
				fromHex(JSON.parse(await fs.readFile(ingressPath, "utf8")).input),
			)
		: undefined;

const store = createStore(directory);
await store.open();
try {
	const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
		store,
		scope: new TextEncoder().encode(
			`documents-checkpoint-recovery-test:v1:${trust.logId}`,
		),
		maxPayloadBytes: MAX_RECOVERY_RECORD_BYTES,
	});
	const generationBefore = checkpoint.current?.generation.toString();
	if (mode === "read" && !generationBefore) {
		throw new Error("Recovery reader found no durable checkpoint");
	}
	const fixture = new CheckpointRecoveryFixture();
	let gateViolations = 0;
	const checkedPhases = [];
	const checkGate = (phase) => {
		checkedPhases.push(phase);
		try {
			fixture.snapshot();
			gateViolations++;
		} catch {
			// Expected until recover completes, including the published hook.
		}
	};
	checkGate("initial");
	const view = await fixture.recover(input, trust, checkpoint, {
		phase: async (phase) => {
			checkGate(phase);
			if (mode !== "write" || phase !== killPhase) return;
			process.stdout.write(
				`${JSON.stringify({
					event: "paused",
					phase,
					generation: checkpoint.current?.generation.toString(),
					gateViolations,
					omittedPrefixReads: fixture.omittedPrefixReads,
					checkedPhases,
				})}\n`,
			);
			// Keep the exact phase in flight until the parent sends SIGKILL;
			// no graceful close or projection publication can run first.
			await new Promise(() => setInterval(() => undefined, 1_000));
		},
	});
	if (mode === "write") {
		throw new Error(`Writer completed without pausing at ${killPhase}`);
	}
	process.stdout.write(
		`${JSON.stringify({
			event: "recovered",
			generationBefore,
			gateViolations,
			omittedPrefixReads: fixture.omittedPrefixReads,
			checkedPhases,
			view,
			snapshot: fixture.snapshot(),
		})}\n`,
	);
} finally {
	await store.close();
}
