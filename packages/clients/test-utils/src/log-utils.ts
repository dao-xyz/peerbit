/* eslint-disable no-console */
import Table from "cli-table3";
import wrapAnsi from "wrap-ansi";

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

	// Format the memory graph, megabytes and progress as a terminal table.

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

	const requestedColumns =
		process.stdout.columns || Number(process.env.COLUMNS) || 80;
	const columns =
		Number.isFinite(requestedColumns) && requestedColumns > 0
			? Math.floor(requestedColumns)
			: 80;
	// Reserve borders and padding, with at least two display columns per cell.
	const contentColumns = Math.max(6, columns - 10);
	const numericColumns = Math.min(18, Math.floor(contentColumns / 3));
	const progressColumns = Math.min(16, Math.floor(contentColumns / 3));
	const contentWidths = [
		Math.min(steps, contentColumns - numericColumns - progressColumns),
		numericColumns,
		progressColumns,
	];
	const wrap = (value: string | number, index: number) =>
		wrapAnsi(String(value), contentWidths[index], { hard: true, trim: false });
	const table = new Table({
		head: ["Memory usage (*)", "Memory bytes (mb)", progressColumnName].map(wrap),
		colWidths: contentWidths.map((width) => width + 2),
		colAligns: ["left", "center", "center"],
		style: { head: ["yellow"], border: [] },
	});
	for (const [i, line] of lines.entries()) {
		table.push(
			[
				colorString(memoryUsages[i].value, line),
				Math.round(memoryUsages[i].value / 1e6),
				memoryUsages[i].progress,
			].map(wrap),
		);
	}

	console.log(table.toString());
	console.log("Max memory usage", Math.round(max / 1e6), "mb");
	console.log("Min memory usage", Math.round(min / 1e6), "mb");
};
