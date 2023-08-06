import { Peerbit } from "peerbit";

const peer = await Peerbit.create();
await peer.bootstrap();
