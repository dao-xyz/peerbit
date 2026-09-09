import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify, stripVTControlCharacters } from "node:util";
import type { RemoteObject } from "../src/remotes.js";

const execFileAsync = promisify(execFile);
const binary = fileURLToPath(new URL("../src/bin.js", import.meta.url));

describe("remote list table rendering", function () {
	this.timeout(30_000);

	const list = async (remotes: RemoteObject[], columns = 420) => {
		const directory = await mkdtemp(join(tmpdir(), "peerbit-remote-table-"));
		try {
			await writeFile(
				join(directory, "remotes.json"),
				JSON.stringify({ remotes }),
			);
			const result = await execFileAsync(
				process.execPath,
				[binary, "remote", "list", "--directory", directory],
				{
					timeout: 15_000,
					maxBuffer: 1024 * 1024,
					env: { ...process.env, FORCE_COLOR: "3", COLUMNS: String(columns) },
				},
			);
			return result.stdout;
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	};

	it("preserves the empty-list message", async () => {
		expect(stripVTControlCharacters(await list([])).trim()).to.equal(
			"No remotes found!",
		);
	});

	it("preserves left-aligned columns, multiline origins and colored identity status", async () => {
		const identity = (await Ed25519Keypair.create()).publicKey
			.toPeerId()
			.toString();
		// Only serve the public identity lookup: rendering must retain unpinned status,
		// not infer verified authentication from an identity string.
		const server = createServer((request, response) => {
			if (request.url === "/peer/id") response.end(identity);
			else {
				response.statusCode = 404;
				response.end();
			}
		});
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(0, "127.0.0.1", resolve);
		});
		try {
			const address = server.address();
			if (!address || typeof address === "string")
				throw new Error("Missing loopback address");
			const endpoint = `http://127.0.0.1:${address.port}`;
			const output = await list([
				{
					name: "alpha",
					group: "production",
					address: endpoint,
					origin: {
						type: "aws",
						region: "eu-north-1",
						instanceId: "i-0123456789",
					},
				},
				{
					name: "beta",
					group: "",
					address: endpoint,
					origin: { type: "hetzner", location: "hel1", serverId: 123456 },
				},
			]);
			expect(output).to.include("\u001b[");
			const lines = stripVTControlCharacters(output)
				.split("\n")
				.filter((line) => line.includes("│"));
			const cells = lines.map((line) => line.split("│").slice(1, -1));
			expect(cells[0].map((cell) => cell.trim())).to.deep.equal([
				"Name",
				"Group",
				"Origin",
				"Status",
				"Address",
			]);
			for (const row of cells.slice(1))
				for (const cell of row) {
					if (cell.trim()) expect(cell).to.match(/^ [^ ]/);
				}
			expect(new Set(lines.map((line) => line.length)).size).to.equal(1);
			const rows = cells.slice(1).map((row) => row.map((cell) => cell.trim()));
			expect(rows).to.deep.include([
				"alpha",
				"production",
				"aws",
				"Y (unpinned)",
				endpoint,
			]);
			expect(rows).to.deep.include(["", "", "eu-north-1", "", ""]);
			expect(rows).to.deep.include(["", "", "i-0123456789", "", ""]);
			expect(rows).to.deep.include([
				"beta",
				"",
				"hetzner",
				"Y (unpinned)",
				endpoint,
			]);
			expect(rows).to.deep.include(["", "", "hel1", "", ""]);
			expect(rows).to.deep.include(["", "", "123456", "", ""]);
			expect(output).not.to.include("ID MISMATCH");
		} finally {
			await new Promise<void>((resolve, reject) =>
				server.close((error) => (error ? reject(error) : resolve())),
			);
		}
	});

	it("wraps ANSI, Unicode, long tokens and multiline origins without loss at 80 columns", async () => {
		const identity = (await Ed25519Keypair.create()).publicKey
			.toPeerId()
			.toString();
		const server = createServer((_request, response) => response.end(identity));
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(0, "127.0.0.1", resolve);
		});
		try {
			const address = server.address();
			if (!address || typeof address === "string")
				throw new Error("Missing loopback address");
			const endpoint = `http://127.0.0.1:${address.port}/${"long-address-token".repeat(6)}`;
			const name = `東京-node-🧪-${"long-name-token".repeat(4)}`;
			const group = "production-group-token".repeat(3);
			const region = "eu-north-region-token".repeat(3);
			const instanceId = `i-${"0123456789".repeat(6)}`;
			const output = await list(
				[
					{
						name: `\u001b[35m${name}\u001b[0m`,
						group,
						address: endpoint,
						origin: { type: "aws", region, instanceId },
					},
				],
				80,
			);
			expect(output).to.include("\u001b[35m");
			const plain = stripVTControlCharacters(output);
			expect(plain).not.to.include("\u001b");
			expect(plain).not.to.include("�");
			expect(plain).not.to.include("…");
			const rows: string[][] = [];
			let data = false;
			for (const line of plain.split("\n")) {
				// This fixture has only ASCII, borders, and these three wide symbols.
				const width = [...line].reduce(
					(sum, character) => sum + ("東京🧪".includes(character) ? 2 : 1),
					0,
				);
				expect(width).to.be.at.most(80);
				if (line.startsWith("├")) data = true;
				if (data && line.includes("│"))
					rows.push(
						line
							.split("│")
							.slice(1, -1)
							.map((cell) => cell.trim()),
					);
			}
			expect(rows.length).to.be.greaterThan(3);
			expect(rows.map((row) => row[0]).join("")).to.equal(name);
			expect(rows.map((row) => row[1]).join("")).to.equal(group);
			expect(rows.map((row) => row[2]).join("")).to.equal(
				`aws${region}${instanceId}`,
			);
			expect(rows.map((row) => row[3]).join("")).to.equal("Y (unpinned)");
			expect(rows.map((row) => row[4]).join("")).to.equal(endpoint);
		} finally {
			await new Promise<void>((resolve, reject) =>
				server.close((error) => (error ? reject(error) : resolve())),
			);
		}
	});
});
