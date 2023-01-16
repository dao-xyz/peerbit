import fs from "fs";
import os from "os";
import path from "path";
import { nanoid } from "nanoid";

/**
 * Temporary folder
 *
 * @param {(uuid: string) => string} [transform=(p) => p] - Transform function to add prefixes or sufixes to the unique id
 * @returns {string} - Full real path to a temporary folder
 */
export const tempdir = (transform = (d) => d) => {
    const osTmpDir = fs.realpathSync(os.tmpdir());
    return path.join(osTmpDir, transform(nanoid()));
};
