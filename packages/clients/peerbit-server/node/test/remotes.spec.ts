import { expect } from "chai";
import fs from "fs";
import os from "os";
import path from "path";
import { Remotes, getRetiredAWSManagementError } from "../src/remotes.js";

describe("legacy remote origins", () => {
	it("gives actionable cleanup details for an AWS origin", () => {
		const error = getRetiredAWSManagementError({
			type: "aws",
			instanceId: "i-0123456789",
			region: "eu-north-1",
		});

		expect(error.message).to.include("i-0123456789");
		expect(error.message).to.include("eu-north-1");
		expect(error.message).to.include("AWS console");
	});
});

describe("remote identity pinning", () => {
	it("persists a server peer ID while still reading legacy entries", async () => {
		const directory = fs.mkdtempSync(
			path.join(os.tmpdir(), "peerbit-remotes-"),
		);
		const remotesPath = path.join(directory, "remotes.json");
		try {
			fs.writeFileSync(
				remotesPath,
				JSON.stringify({
					remotes: [
						{
							name: "legacy",
							address: "https://legacy.example",
							group: "default",
						},
					],
				}),
			);
			const remotes = new Remotes(remotesPath);
			const legacy = remotes.getByName("legacy")!;
			expect(legacy.peerId).to.equal(undefined);
			remotes.add({ ...legacy, peerId: "12D3KooWPinned" });
			expect(new Remotes(remotesPath).getByName("legacy")?.peerId).to.equal(
				"12D3KooWPinned",
			);
		} finally {
			fs.rmSync(directory, { recursive: true, force: true });
		}
	});

	it("requires removal before replacing a pinned identity", () => {
		const directory = fs.mkdtempSync(
			path.join(os.tmpdir(), "peerbit-remotes-"),
		);
		const remotesPath = path.join(directory, "remotes.json");
		try {
			const remotes = new Remotes(remotesPath);
			remotes.add({
				name: "production",
				address: "https://node-a.example",
				group: "default",
				peerId: "server-a",
			});

			expect(() =>
				remotes.add({
					name: "production",
					address: "https://node-b.example",
					group: "default",
					peerId: "server-b",
				}),
			).to.throw("Remove it before replacing the pin");
			expect(new Remotes(remotesPath).getByName("production")).to.deep.equal({
				name: "production",
				address: "https://node-a.example",
				group: "default",
				peerId: "server-a",
			});

			expect(remotes.remove("production")).to.equal(true);
			remotes.add({
				name: "production",
				address: "https://node-b.example",
				group: "default",
				peerId: "server-b",
			});
			expect(new Remotes(remotesPath).getByName("production")?.peerId).to.equal(
				"server-b",
			);
		} finally {
			fs.rmSync(directory, { recursive: true, force: true });
		}
	});
});
