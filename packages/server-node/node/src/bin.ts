#!/usr/bin/env node
import { cli } from "./cli.js";
try {
    await cli();
} catch (error: any) {
    throw new Error("Unexpected error: " + error?.message);
}
