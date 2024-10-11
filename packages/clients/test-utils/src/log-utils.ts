/* eslint-disable no-console */
import Table from "tty-table";

export const printMemoryUsage = (
	memoryUsages: { value: number; progress: number }[],
	progressColumnName: string = "progress",
) => {
	// do ascii graph
	let max = Math.max(...memoryUsages.map((x) => x.value));
	let min = Math.min(...memoryUsages.map((x) => x.value));
	let range = max - min;
	let steps = 300;
	let step = range / steps;
	let buckets = Array.from({ length: steps }, (_, i) => {
		return min + i * step;
	});
	let lines = memoryUsages.map((memory) => {
		/*  let bucket = Math.floor((memory - min) / step) */
		return Array.from({ length: steps }, (_, i) => {
			return memory.value > buckets[i] ? "█" : " ";
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
			{ value: progressColumnName },
		],
		lines.map((line, i) => {
			return [
				{ value: colorString(memoryUsages[i].value, line) },
				{ value: Math.round(memoryUsages[i].value / 1e6) },
				{ value: memoryUsages[i].progress /* insertBatchSize * (i + 1) */ },
			];
		}),
	);

	console.log(table.render());
	console.log("Max memory usage", Math.round(max / 1e6), "mb");
	console.log("Min memory usage", Math.round(min / 1e6), "mb");
};
