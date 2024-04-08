import { tests } from "@peerbit/document-tests";
import { SQLLiteEngine } from "../src/index.js";
import sqlite3 from "../src/sqlite3.js";


tests(() => new SQLLiteEngine({ createDatabase: sqlite3.createDatabase }));
