import path, { dirname } from "path";
import { fileURLToPath } from "url";

// eslint-disable-next-line @typescript-eslint/naming-convention
export const __dirname = dirname(fileURLToPath(import.meta.url));
export const modulesPath = path.join(__dirname, "./tmp/cli-test/modules");
