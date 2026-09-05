import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
	appendFile,
	cp,
	mkdir,
	mkdtemp,
	readFile,
	realpath,
	rm,
	symlink,
	writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const blocksSource = join(root, "packages/transport/blocks-interface");
const streamSource = join(root, "packages/transport/stream-interface");
const cryptoSource = join(root, "packages/utils/crypto");
const temporaryRoot = await mkdtemp(join(tmpdir(), "peerbit-blocks-import-"));

const run = (command, args, cwd = root) => {
	const result = spawnSync(command, args, {
		cwd,
		encoding: "utf8",
		timeout: 60_000,
	});
	assert.equal(result.error, undefined, result.error?.message);
	assert.equal(result.status, 0, result.stdout + result.stderr);
	return result.stdout;
};

const linkDependency = async (source, target) => {
	await mkdir(dirname(target), { recursive: true });
	await symlink(await realpath(source), target, "junction");
};

// Reuse the frozen installed dependencies without installing, deduping, or
// contacting a registry. Copy published JS so Node sees two genuine crypto
// module identities, not two symlinks to one canonical module URL.
const copyRuntime = async (source, target) => {
	await mkdir(target, { recursive: true });
	await cp(join(source, "package.json"), join(target, "package.json"));
	await cp(join(source, "dist/src"), join(target, "dist/src"), {
		recursive: true,
	});
};
const linkDependencies = async (source, target, omit = []) => {
	const manifest = JSON.parse(await readFile(join(source, "package.json")));
	for (const name of Object.keys(manifest.dependencies ?? {})) {
		if (!omit.includes(name)) {
			await linkDependency(
				join(source, "node_modules", name),
				join(target, "node_modules", name),
			);
		}
	}
};

try {
	const tarballs = join(temporaryRoot, "tarballs");
	const consumer = join(temporaryRoot, "consumer");
	const modules = join(consumer, "node_modules");
	const blocks = join(modules, "@peerbit/blocks-interface");
	const stream = join(modules, "@peerbit/stream-interface");
	const canonicalCrypto = join(modules, "@peerbit/crypto");
	const nestedCrypto = join(stream, "node_modules/@peerbit/crypto");
	await mkdir(tarballs);
	await mkdir(blocks, { recursive: true });
	const packed = JSON.parse(
		run("pnpm", [
			"--dir",
			blocksSource,
			"pack",
			"--pack-destination",
			tarballs,
			"--json",
		]),
	);
	assert.equal(packed.name, "@peerbit/blocks-interface");
	run("tar", ["-xzf", packed.filename, "--strip-components=1", "-C", blocks]);
	await copyRuntime(streamSource, stream);
	await copyRuntime(cryptoSource, canonicalCrypto);
	await copyRuntime(cryptoSource, nestedCrypto);
	await linkDependencies(blocksSource, blocks, [
		"@peerbit/stream-interface",
		"@peerbit/crypto",
	]);
	await linkDependencies(streamSource, stream, ["@peerbit/crypto"]);
	await linkDependencies(cryptoSource, canonicalCrypto);
	await linkDependencies(cryptoSource, nestedCrypto);
	await linkDependency(
		join(cryptoSource, "node_modules/@dao-xyz/borsh"),
		join(modules, "@dao-xyz/borsh"),
	);
	// Instrument only the copied nested artifact. The positive probe must not
	// evaluate it; the exact legacy empty-import edge is the negative control.
	await appendFile(
		join(nestedCrypto, "dist/src/index.js"),
		"\nglobalThis.__peerbitNestedCryptoEvaluated = true;\n",
	);
	await writeFile(
		join(consumer, "legacy-edge.mjs"),
		'import {} from "@peerbit/stream-interface";\n',
	);
	await writeFile(
		join(consumer, "probe.mjs"),
		`
import assert from "node:assert/strict";
import { serialize, deserialize, getSchema } from "@dao-xyz/borsh";
import { Ed25519PublicKey, PublicSignKey } from "@peerbit/crypto";

const key = new Ed25519PublicKey({ publicKey: new Uint8Array(32).fill(7) });
const schema = getSchema(Ed25519PublicKey);
assert(schema, "the real canonical crypto schema must be registered");
const encoded = serialize(key);
assert.equal(globalThis.__peerbitNestedCryptoEvaluated, undefined);
const blocks = await import("@peerbit/blocks-interface");
assert.equal(globalThis.__peerbitNestedCryptoEvaluated, undefined,
  "blocks-interface must not evaluate the hoisted stream/nested crypto graph");
assert.equal(getSchema(Ed25519PublicKey), schema);
const decoded = deserialize(encoded, PublicSignKey);
assert.equal(decoded.constructor, Ed25519PublicKey);
assert(decoded.equals(key));
assert.deepEqual(serialize(decoded), encoded);
const bytes = new Uint8Array([1, 2, 3]);
const block = await blocks.createBlock(bytes, "raw");
await blocks.verifyBlockBytes(block.cid, bytes);

await import("./legacy-edge.mjs");
assert.equal(globalThis.__peerbitNestedCryptoEvaluated, true,
  "the legacy empty-import control must actually evaluate the nested graph");
const nested = await import("./node_modules/@peerbit/stream-interface/node_modules/@peerbit/crypto/dist/src/index.js");
assert.notEqual(nested.Ed25519PublicKey, Ed25519PublicKey);
assert.notEqual(getSchema(nested.Ed25519PublicKey), schema);
const nestedDecoded = deserialize(encoded, nested.PublicSignKey);
assert.equal(nestedDecoded.constructor, nested.Ed25519PublicKey);
assert.equal(nestedDecoded instanceof Ed25519PublicKey, false);
console.log("Packed blocks import avoids nested crypto evaluation; constructor/schema identity and legacy control verified.");
`,
	);
	console.log(
		run(process.execPath, [join(consumer, "probe.mjs")], consumer).trim(),
	);
} finally {
	await rm(temporaryRoot, { recursive: true, force: true });
}
