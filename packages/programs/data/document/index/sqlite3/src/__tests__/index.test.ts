import { tests } from "@peerbit/document-tests";
import { SQLLiteEngine } from "../index.js";
/* import { sqlite } from '../node-sqlite3.js'; */
import sqlite from "sqlite3";

tests(() => new SQLLiteEngine(sqlite as any));
