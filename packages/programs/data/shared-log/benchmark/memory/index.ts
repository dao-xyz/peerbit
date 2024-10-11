// Run with "node --loader ts-node/esm ./benchmark/memory/index.ts"
// run child.ts with ts-node in a subprocess so we can measure memory consumption
import { printMemoryUsage } from "@peerbit/test-utils/log-utils.js";
import { fork } from "child_process";
import { dirname, resolve } from "path";
import pidusage from "pidusage";
import { fileURLToPath } from "url";
import type { Message } from "./utils.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const child = fork(resolve(__dirname, "child.ts"), [], {
	stdio: ["pipe", "pipe", "pipe", "ipc"],
});

const ramMemoryUsage = () => {
	return new Promise<number>((resolve) => {
		pidusage(child.pid!, (err, stats) => {
			if (err) {
				console.error(err);
				resolve(0);
			} else {
				resolve(stats.memory);
			}
		});
	});
};

child.stdout!.on("data", (data) => {
	// log without newline
	process.stdout.write(data.toString());
});

child.stderr!.on("data", (data) => {
	// log without newline
	process.stderr.write(data.toString());
});

child.on("close", (code) => {
	console.log(`child process exited with code ${code}`);
});

// wait for the child process to exit

child.on("exit", (code) => {
	process.exit(code!);
});

// wait for child to be ready
const waitForReady = () => {
	return new Promise<void>((resolve) => {
		let listener = (data: any) => {
			try {
				const message = JSON.parse(data.toString()) as Message;
				if (message.type === "ready") {
					child.off("message", listener);
					resolve();
				}
			} catch (error) {}
		};
		child.on("message", listener);
	});
};
await waitForReady();

child.send({ type: "init", storage: "disc" });

await waitForReady();

// insert docs into the store by sending a message to the child

const send = (message: Message) => {
	child.send(message);
};

// send the insert message
let inserts = 150;
let insertBatchSize = 10;
let memoryUsages: number[] = [];
let size = 1e6;
console.log(
	"Inserting batches of",
	insertBatchSize,
	"documents",
	inserts,
	"times",
	"with",
	size / 1e6,
	"MB size",
);
for (let i = 0; i < inserts; i++) {
	// log a progress bar that is updating without printing newline
	process.stdout.write(`\r${i}/${inserts}`);

	send({ type: "insert", docs: insertBatchSize, size });

	await waitForReady();
	let memory = await ramMemoryUsage();
	memoryUsages.push(memory);
}

printMemoryUsage(
	memoryUsages.map((x, i) => {
		return { value: x, progress: insertBatchSize * (i + 1) };
	}),
	"# of inserts",
);

child.kill();
