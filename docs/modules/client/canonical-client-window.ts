import { connectWindow } from "@peerbit/canonical-client/window";

const canonical = await connectWindow({
	channel: "my-canonical-channel",
	targetOrigin: "*",
});

console.log(await canonical.peerId());
console.log(canonical.identity.publicKey.toString());
