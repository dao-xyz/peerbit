import path, { dirname } from "path";
import { fileURLToPath } from "url";

export const __dirname = dirname(fileURLToPath(import.meta.url));
export const modulesPath = path.join(__dirname, "./tmp/cli-test/modules");