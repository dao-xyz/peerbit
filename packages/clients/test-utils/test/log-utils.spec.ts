import { expect } from "chai";
import { stripVTControlCharacters } from "node:util";
import sinon from "sinon";
import { printMemoryUsage } from "../src/log-utils.js";

describe("memory usage table rendering", () => {
	it("preserves headers, values, ANSI bars and visible column widths", () => {
		const columns = Object.getOwnPropertyDescriptor(process.stdout, "columns");
		const output = sinon.stub(console, "log");
		Object.defineProperty(process.stdout, "columns", {
			configurable: true,
			value: 420,
		});
		try {
			printMemoryUsage(
				[
					{ value: 1_000_000, progress: 7 },
					{ value: 4_500_000, progress: 11 },
					{ value: 8_000_000, progress: 23 },
				],
				"Documents",
			);
			expect(output.firstCall.args).to.deep.equal(["Memory Usage Graph"]);
			const table = output.secondCall.args[0] as string;
			expect(table).to.include("\u001b[38;2;");
			const lines = stripVTControlCharacters(table).split("\n");
			const rows = lines
				.filter((line) => line.includes("│"))
				.map((line) =>
					line
						.split("│")
						.slice(1, -1)
						.map((cell) => cell.trim()),
				);
			expect(rows[0]).to.deep.equal([
				"Memory usage (*)",
				"Memory bytes (mb)",
				"Documents",
			]);
			expect(rows.slice(1).map((row) => row.slice(1))).to.deep.equal([
				["1", "7"],
				["5", "11"],
				["8", "23"],
			]);
			expect(rows.slice(1).map((row) => row[0].length)).to.deep.equal([
				0, 150, 300,
			]);
			const widths = lines
				.filter((line) => line.includes("│"))
				.map((line) => line.length);
			expect(new Set(widths).size).to.equal(1);
			expect(output.getCall(2).args).to.deep.equal([
				"Max memory usage",
				8,
				"mb",
			]);
			expect(output.getCall(3).args).to.deep.equal([
				"Min memory usage",
				1,
				"mb",
			]);
		} finally {
			output.restore();
			if (columns) Object.defineProperty(process.stdout, "columns", columns);
			else delete process.stdout.columns;
		}
	});

	it("keeps the default progress heading", () => {
		const columns = Object.getOwnPropertyDescriptor(process.stdout, "columns");
		const output = sinon.stub(console, "log");
		Object.defineProperty(process.stdout, "columns", {
			configurable: true,
			value: 420,
		});
		try {
			printMemoryUsage([
				{ value: 0, progress: 1 },
				{ value: 1_000_000, progress: 2 },
			]);
			expect(
				stripVTControlCharacters(output.secondCall.args[0] as string),
			).to.include("progress");
		} finally {
			output.restore();
			if (columns) Object.defineProperty(process.stdout, "columns", columns);
			else delete process.stdout.columns;
		}
	});

	it("wraps every colored bar and long multiline heading within 80 columns", () => {
		const columns = Object.getOwnPropertyDescriptor(process.stdout, "columns");
		const output = sinon.stub(console, "log");
		Object.defineProperty(process.stdout, "columns", {
			configurable: true,
			value: 80,
		});
		try {
			const heading = "PublishedDocumentsByPeer-1234567890";
			printMemoryUsage(
				[
					{ value: 0, progress: 17 },
					{ value: 1_000_000, progress: 29 },
				],
				`\u001b[35m${heading}\nnext\u001b[0m`,
			);
			const table = output.secondCall.args[0] as string;
			expect(table).to.include("\u001b[38;2;");
			const plain = stripVTControlCharacters(table);
			expect(plain).not.to.include("\u001b");
			expect(plain).not.to.include("…");
			const sections: string[][][] = [[]];
			for (const line of plain.split("\n")) {
				expect(line.length).to.be.at.most(80);
				if (line.startsWith("├")) sections.push([]);
				if (line.includes("│"))
					sections.at(-1)!.push(
						line
							.split("│")
							.slice(1, -1)
							.map((cell) => cell.trim()),
					);
			}
			expect(sections).to.have.length(3);
			expect(sections[0].map((row) => row[2]).join("")).to.equal(
				`${heading}next`,
			);
			expect(sections[1].map((row) => row[1]).join("")).to.equal("0");
			expect(sections[1].map((row) => row[2]).join("")).to.equal("17");
			expect(sections[2].map((row) => row[0]).join("")).to.equal(
				"█".repeat(300),
			);
			expect(sections[2].map((row) => row[1]).join("")).to.equal("1");
			expect(sections[2].map((row) => row[2]).join("")).to.equal("29");
		} finally {
			output.restore();
			if (columns) Object.defineProperty(process.stdout, "columns", columns);
			else delete process.stdout.columns;
		}
	});
});
