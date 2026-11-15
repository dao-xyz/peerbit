import assert from "node:assert/strict";
import { readFileSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// The optional anchor lets the packed-consumer gate exercise its own install.
// Never resolve WebRTC or node-datachannel from the repository root.
const peerbitAnchor =
	process.argv[2] ??
	fileURLToPath(
		new URL("../../packages/clients/peerbit/package.json", import.meta.url),
	);
const peerbitRequire = createRequire(peerbitAnchor);
const webrtcEntry = peerbitRequire.resolve("@libp2p/webrtc");
const webrtcRequire = createRequire(webrtcEntry);
const nativeEntry = webrtcRequire.resolve("node-datachannel");
const webrtcPackage = JSON.parse(
	readFileSync(resolve(dirname(webrtcEntry), "../../package.json"), "utf8"),
);
const nativePackage = JSON.parse(
	readFileSync(resolve(dirname(nativeEntry), "../../../package.json"), "utf8"),
);
assert.equal(webrtcPackage.name, "@libp2p/webrtc");
assert.equal(nativePackage.name, "node-datachannel");
assert.match(nativePackage.version, /^\d+\.\d+\.\d+$/);
const [major, minor, patch] = nativePackage.version.split(".").map(Number);
assert.ok(
	major > 0 || minor > 33 || (minor === 33 && patch >= 4),
	`WebRTC resolved node-datachannel ${nativePackage.version}; need >=0.33.4`,
);

const native = webrtcRequire(nativeEntry);
const binding = Object.values(webrtcRequire.cache).find(
	(module) =>
		module?.filename.endsWith(".node") &&
		module.exports.PeerConnection === native.PeerConnection,
);
assert.ok(binding, "node-datachannel did not load its native PeerConnection");
console.log(
	JSON.stringify({
		phase: "native-webrtc-loaded",
		node: process.version,
		webrtc: webrtcPackage.version,
		nodeDatachannel: nativePackage.version,
		libdatachannel: native.getLibraryVersion(),
		webrtcEntry: realpathSync(webrtcEntry),
		nativeEntry: realpathSync(nativeEntry),
		binding: realpathSync(binding.filename),
	}),
);

const peer = new native.PeerConnection("peerbit-lifecycle", { iceServers: [] });
const closed = new Promise((resolve) => {
	peer.onStateChange((state) => {
		if (state === "closed") resolve();
	});
});
try {
	assert.equal(peer.state(), "new");
} finally {
	peer.close();
}
await closed;
// Do not call global cleanup or process.exit: the parent proves natural exit.
console.log(JSON.stringify({ phase: "native-webrtc-closed" }));
