#!/usr/bin/env node
import { startServerWithNode } from "./api.js";
try {
    const _node = await startServerWithNode(false);
} catch (error: any) {
    console.error("Error: " + error?.message);
}
