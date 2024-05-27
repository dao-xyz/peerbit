import DB from "better-sqlite3";
import type { Database as IDatabase } from "./types.js";
import fs from 'fs'

let create = async (directory?: string) => {

  let dbFileName: string;
  if (directory) {
    // if directory is provided, check if directory exist, if not create it
    if (!fs.existsSync(directory)) {
      fs.mkdirSync(directory, { recursive: true });
    }
    dbFileName = `${directory}/db.sqlite`;
  }
  else {
    dbFileName = ":memory:";
  }

  const db = new DB(dbFileName, { fileMustExist: false, readonly: false/* , verbose: (message) => console.log(message)  */ });
  db.pragma('journal_mode = WAL');
  db.pragma("foreign_keys = on")
  return db as any as IDatabase; // TODO fix this
}

export { create }
