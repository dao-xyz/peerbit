// Packs native (wasm-bindgen) packages with `pnpm pack`, installs the tarballs
// into a throwaway project and imports them from there, asserting that the
// shipped wasm artifacts actually load and work outside the workspace.
//
// Usage:
//   node ./scripts/native-pack-smoke.mjs                       # all default targets
//   node ./scripts/native-pack-smoke.mjs @peerbit/native-backbone
//
// The packages (and their workspace dependencies) must be built first
// (`pnpm --filter <name>... run build`), since `pnpm pack` ships `dist`.
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const SMOKES = {
	"@peerbit/any-store-rust": `
import { strict as assert } from "node:assert";
import { createStore } from "@peerbit/any-store-rust";

const store = createStore(); // in-memory store, still backed by wasm
await store.open();
const value = Uint8Array.from([1, 2, 3, 4]);
await store.put("smoke-key", value);
const read = await store.get("smoke-key");
assert(read, "expected the wasm store to return the stored value");
assert.deepEqual([...read], [...value]);
assert((await store.size()) > 0);
await store.close();
console.log("@peerbit/any-store-rust tarball smoke OK");
`,
	"@peerbit/native-backbone": `
import { strict as assert } from "node:assert";
import { createNativePeerbitBackbone } from "@peerbit/native-backbone";

const fromHex = (hex) =>
	Uint8Array.from(hex.match(/.{2}/g).map((byte) => Number.parseInt(byte, 16)));
// RFC 8032 ed25519 test vector, same fixture as the package test suite.
const privateKey = fromHex(
	"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
);
const publicKey = fromHex(
	"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
);

const backbone = await createNativePeerbitBackbone({
	clockId: publicKey,
	privateKey,
	publicKey,
});
assert.equal(backbone.logLength, 0);
const bytes = Uint8Array.from([5, 6, 7, 8]);
const cid = await backbone.blocks.put(bytes);
assert(typeof cid === "string" && cid.length > 0);
const read = backbone.blocks.get(cid);
assert(read, "expected the wasm block store to return the stored block");
assert.deepEqual([...read], [...bytes]);
console.log("@peerbit/native-backbone tarball smoke OK");
`,
	"@peerbit/network-rust": `
import { strict as assert } from "node:assert";
import {
	createNativeWire,
	createRustCoreStream,
	readNativeWireFrameRecord,
} from "@peerbit/network-rust";

const wire = await createNativeWire();
const frames = wire.testCorpusFrames();
assert(frames.length > 0, "expected the wasm module to emit corpus frames");
const records = wire.decodeAndVerifyBatch(frames, Date.now());
const first = readNativeWireFrameRecord(records, 0);
assert.equal(first.decodeOk, true);
assert.equal(first.verifyStatus, 1); // ed25519 signature verified in wasm
assert.deepEqual([...wire.reencodeFrame(frames[0])], [...frames[0]]);

// The grown package: the full DirectStream core plus the protocol ports.
const core = await createRustCoreStream();
assert(core.nativeWire, "expected the core to expose the native wire module");

const routes = core.createRoutes({ me: "me", routeMaxRetentionPeriod: 1000 });
routes.updateSession("target", 0);
routes.add("me", "n1", "target", 1, 0, 0);
assert.equal(routes.findNeighbor("me", "target")?.list[0]?.hash, "n1");
assert.equal(routes.isReachable("me", "target"), true);
assert.equal(routes.hasTarget("target"), true);

const seen = core.createSeenCache({ max: 100, ttl: 1000 });
assert.equal(seen.modify(frames[0], 0), 0);
assert.equal(seen.modify(frames[0], 0), 1);

const blockRequest = core.blockExchange.encodeBlockRequest(
	"zb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA",
);
const decodedBlock = core.blockExchange.decodeBlockMessage(blockRequest);
assert.equal(decodedBlock.type, "request");

const subscribe = core.topicControl.encodeSubscribe(["topic-a"], true);
const decodedSubscribe = core.topicControl.decodePubSubMessage(subscribe);
assert.equal(decodedSubscribe.type, "subscribe");
assert.deepEqual(decodedSubscribe.topics, ["topic-a"]);

const channelKey = new Uint8Array(32).fill(7);
const joinReq = core.fanout.encodeJoinReq(channelKey, 42, 1);
const decodedJoin = core.fanout.decodeJoinReq(joinReq);
assert.equal(decodedJoin?.reqId, 42);
console.log("@peerbit/network-rust tarball smoke OK");
`,
};

const run = (cmd, args, options = {}) =>
	execFileSync(cmd, args, { stdio: "inherit", ...options });

const listWorkspacePackages = () => {
	const json = execFileSync("pnpm", ["-r", "list", "--depth", "-1", "--json"], {
		cwd: rootDir,
		encoding: "utf8",
		maxBuffer: 64 * 1024 * 1024,
	});
	const byName = new Map();
	for (const pkg of JSON.parse(json)) {
		if (pkg.name) byName.set(pkg.name, pkg.path);
	}
	return byName;
};

// Workspace dependency closure: these tarballs must be installed alongside the
// target so npm resolves `workspace:*` deps from the packed (local) versions
// instead of the registry.
const workspaceClosure = (name, byName) => {
	const seen = new Set();
	const visit = (pkgName) => {
		if (seen.has(pkgName)) return;
		seen.add(pkgName);
		const dir = byName.get(pkgName);
		if (!dir) throw new Error(`workspace package not found: ${pkgName}`);
		const manifest = JSON.parse(
			fs.readFileSync(path.join(dir, "package.json"), "utf8"),
		);
		for (const [dep, spec] of Object.entries(manifest.dependencies ?? {})) {
			if (byName.has(dep) && String(spec).startsWith("workspace:")) visit(dep);
		}
	};
	visit(name);
	return [...seen];
};

const packPackage = (name, dir, destDir) => {
	const entry = path.join(dir, "dist", "src", "index.js");
	if (!fs.existsSync(entry)) {
		throw new Error(
			`${name} is not built (missing ${entry}). Run \`pnpm --filter ${name}... run build\` first.`,
		);
	}
	fs.mkdirSync(destDir, { recursive: true });
	run("pnpm", ["pack", "--pack-destination", destDir], { cwd: dir });
	const tarballs = fs.readdirSync(destDir).filter((f) => f.endsWith(".tgz"));
	if (tarballs.length !== 1) {
		throw new Error(
			`expected exactly one tarball for ${name} in ${destDir}, found: ${tarballs.join(", ")}`,
		);
	}
	return path.join(destDir, tarballs[0]);
};

const targets = process.argv.slice(2).length
	? process.argv.slice(2)
	: Object.keys(SMOKES);

for (const target of targets) {
	if (!SMOKES[target]) {
		throw new Error(
			`no smoke defined for ${target}; known targets: ${Object.keys(SMOKES).join(", ")}`,
		);
	}
}

const byName = listWorkspacePackages();
const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "native-pack-smoke-"));
console.log(`native-pack-smoke: working directory ${workDir}`);

try {
	for (const target of targets) {
		console.log(`\n=== ${target} ===`);
		const closure = workspaceClosure(target, byName);
		console.log(`packing workspace closure: ${closure.join(", ")}`);
		const targetDir = path.join(
			workDir,
			target.replace(/[^a-zA-Z0-9-]+/g, "_"),
		);
		const tarballs = closure.map((name, i) =>
			packPackage(name, byName.get(name), path.join(targetDir, `pack-${i}`)),
		);

		const appDir = path.join(targetDir, "app");
		fs.mkdirSync(appDir, { recursive: true });
		fs.writeFileSync(
			path.join(appDir, "package.json"),
			JSON.stringify(
				{ name: "native-pack-smoke", private: true, type: "module" },
				null,
				2,
			),
		);
		run(
			"npm",
			["install", "--no-audit", "--no-fund", "--loglevel=error", ...tarballs],
			{ cwd: appDir },
		);

		const smokeFile = path.join(appDir, "smoke.mjs");
		fs.writeFileSync(smokeFile, SMOKES[target].trimStart());
		run("node", ["smoke.mjs"], { cwd: appDir });
	}
	fs.rmSync(workDir, { recursive: true, force: true });
	console.log("\nnative-pack-smoke: all targets OK");
} catch (err) {
	console.error(`\nnative-pack-smoke failed; artifacts kept at ${workDir}`);
	throw err;
}
