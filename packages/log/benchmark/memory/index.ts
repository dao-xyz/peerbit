// Run with "node --loader ts-node/esm ./benchmark/memory/index.ts"
// run insert.ts with ts-node in a subprocess so we can measure memory consumption
import { fork } from "child_process";
import { dirname, resolve } from "path";
import pidusage from "pidusage";
import Table from "tty-table";
import { fileURLToPath } from "url";
import type { Message } from "./utils.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const child = fork(resolve(__dirname, "insert.ts"), [], {
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
let inserts = 60;
let insertBatchSize = 1000;
let memoryUsages: number[] = [];
console.log(
	"Inserting batches of",
	insertBatchSize,
	"documents",
	inserts,
	"times",
);
for (let i = 0; i < inserts; i++) {
	// log a progress bar that is updating without printing newline
	process.stdout.write(`\r${i}/${inserts}`);

	send({ type: "insert", docs: insertBatchSize, size: 1024 });

	await waitForReady();
	let memory = await ramMemoryUsage();
	memoryUsages.push(memory);
}

// do ascii graph
let max = Math.max(...memoryUsages);
let min = Math.min(...memoryUsages);
let range = max - min;
let steps = 300;
let step = range / steps;
let buckets = Array.from({ length: steps }, (_, i) => {
	return min + i * step;
});
let lines = memoryUsages.map((memory) => {
	/*  let bucket = Math.floor((memory - min) / step) */
	return Array.from({ length: steps }, (_, i) => {
		return memory > buckets[i] ? "█" : " ";
	}).join("");
});

console.log("Memory Usage Graph");

// do a nicely tty-table formatted table with "Memory ascii", "Memory bytes (mb)", "# of inserts".

const colorString = (bytes: number, string: string) => {
	// color encode byte values so that the highest get red color and lowest get green color
	// and values in between get a color in in shades of red and green
	let colors = Array.from({ length: steps + 1 }, (_, i) => {
		let r = Math.floor(255 * (i / steps));
		let g = Math.floor(255 * ((steps - i) / steps));
		let b = 0;
		return `38;2;${r};${g};${b}`;
	});
	let bucket = Math.floor((bytes - min) / step);
	let color = colors[bucket];
	return `\x1b[${color}m${string}\x1b[0m`;
};

let table = Table(
	[
		{ value: "Memory usage (*)", width: steps + 2, align: "left" },
		{ value: "Memory bytes (mb)" },
		{ value: "# of inserts" },
	],
	lines.map((line, i) => {
		return [
			{ value: colorString(memoryUsages[i], line) },
			{ value: Math.round(memoryUsages[i] / 1e6) },
			{ value: insertBatchSize * (i + 1) },
		];
	}),
);

console.log(table.render());
console.log("Max memory usage", Math.round(max / 1e6), "mb");
console.log("Min memory usage", Math.round(min / 1e6), "mb");

child.kill();
