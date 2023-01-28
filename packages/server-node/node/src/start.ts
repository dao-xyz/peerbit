#!/usr/bin/env node
import { createNode } from "./libp2p.js";
try {
    const _node = await createNode();
} catch (error: any) {
    console.error("Error: " + error?.message);
}
