// B12 retirement pin (eager tombstone registration). The five legacy
// replication-info wire variants ([1,0]-[1,4]) are permanent decode
// tombstones: their classes, @variant registrations and the public
// `export * from "./replication.js"` entry survive the compatibility-mode
// retirement. The codec suite's byte pin can pass merely because some other
// suite import happened to register the classes first. This guard decodes the
// frozen bytes in a FRESH Node process whose ONLY import is the public
// package entry — no compatibility open, no prior suite imports — so
// tombstone decode can never silently start depending on retired plumbing or
// on incidental registration by unrelated modules.
import { expect } from "chai";
import { execFile } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

// Frozen wire bytes. The ResponseRoleMessage/Observer row is the identical
// literal to the codec suite's tombstone pin ("keeps every legacy
// replication-info variant tag byte-identical"); the bytes must never change
// again.
//
// ResponseRoleMessage carries a nested Role, so ONE message row does not cover
// its decode surface: each Role variant is separately @variant-registered and
// separately reachable from persisted frames. NoType in particular has zero
// references anywhere outside its own declaration — an unpinned decode path is
// the only thing that keeps it registered, which is exactly the state in which
// a class quietly disappears. All three variants are pinned here.
//
// Third tuple element = expected nested Role class, where the row has one.
const FROZEN_TOMBSTONES = [
	["000100", "RequestReplicationInfoMessage"],
	["0001010100", "ResponseRoleMessage", "NoType"],
	["0001010101", "ResponseRoleMessage", "Observer"],
	[
		"00010101020100000000000000000000000000000000000000",
		"ResponseRoleMessage",
		"Replicator",
	],
	["00010200000000", "AllReplicatingSegmentsMessage"],
	["00010300000000", "AddedReplicationSegmentMessage"],
	["00010400000000", "StoppedReplicating"],
] as const;

describe("receive admission replication-info tombstone registration", () => {
	it("decodes every frozen tombstone in a fresh process via the package entry alone", async function () {
		this.timeout(60_000);
		// dist/test/<spec>.js -> package root.
		const packageRoot = path.resolve(
			path.dirname(fileURLToPath(import.meta.url)),
			"..",
			"..",
		);
		const script = `
import { deserialize, serialize } from "@dao-xyz/borsh";
const entry = await import("@peerbit/shared-log");
// TransportMessage is deliberately not part of the export surface; reach the
// decode base class through a tombstone's prototype chain instead of a deep
// import so this stays a pure package-entry consumer.
const TransportMessage = Object.getPrototypeOf(entry.RequestReplicationInfoMessage);
const cases = ${JSON.stringify(FROZEN_TOMBSTONES)};
for (const [hex, expectedName, expectedRoleName] of cases) {
	const label = expectedRoleName ? expectedName + "/" + expectedRoleName : expectedName;
	const bytes = Uint8Array.from(Buffer.from(hex, "hex"));
	const decoded = deserialize(bytes, TransportMessage);
	if (decoded.constructor.name !== expectedName) {
		throw new Error(
			"tombstone " + hex + " decoded to " + decoded.constructor.name,
		);
	}
	if (expectedRoleName && decoded.role?.constructor?.name !== expectedRoleName) {
		throw new Error(
			"tombstone " + hex + " decoded role " + decoded.role?.constructor?.name,
		);
	}
	const reserialized = Buffer.from(serialize(decoded)).toString("hex");
	if (reserialized !== hex) {
		throw new Error(
			"tombstone " + label + " reserialized to " + reserialized,
		);
	}
	console.log(label + " OK");
}
`;
		const { stdout } = await execFileAsync(
			process.execPath,
			["--input-type=module", "-e", script],
			{ cwd: packageRoot, timeout: 45_000 },
		);
		for (const [, expectedName, expectedRoleName] of FROZEN_TOMBSTONES) {
			expect(stdout).to.include(
				`${expectedRoleName ? `${expectedName}/${expectedRoleName}` : expectedName} OK`,
			);
		}
	});
});
