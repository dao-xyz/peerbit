#!/usr/bin/env node
import { createNode } from "./libp2p.js";
const node = await createNode();
console.log("Starting node with address(es): ");
const id = await node.peerId.toString();
console.log("id: " + id);
console.log("Addresses: ");
node.getMultiaddrs().forEach((addr) => {
    console.log(addr.toString());
});
