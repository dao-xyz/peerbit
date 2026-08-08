import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
	IMAGE_SIZE_EXCEPTION_CVES,
	IMAGE_SIZE_EXCEPTION_EXPIRES_AT,
	validateImageSizePnpmLockException,
} from "./image-size-advisory-exception.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const lockText = await readFile(
	resolve(repositoryRoot, "pnpm-lock.yaml"),
	"utf8",
);
validateImageSizePnpmLockException(lockText);
console.log(
	"Validated the committed pnpm image-size exception graph for " +
		IMAGE_SIZE_EXCEPTION_CVES.join(" and ") +
		"; the exception expires at " +
		IMAGE_SIZE_EXCEPTION_EXPIRES_AT +
		".",
);
