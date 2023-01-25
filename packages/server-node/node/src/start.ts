#!/usr/bin/env node
import { createNode } from "./libp2p.js";
try {
	const _node = await createNode();
	_node.pubsub.subscribe("world");
	_node.pubsub.subscribe("world!");
	_node.pubsub.subscribe("_block");
} catch (error: any) {
	console.error("Error: " + error?.message);
}
