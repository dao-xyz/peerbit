import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

const repoRoot = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"..",
);
const validatorFiles = new Set([
	"scripts/validate-retired-domain-references.mjs",
	"scripts/validate-retired-domain-references.test.mjs",
]);
const retiredDomainPattern = /(?:dao\.xyz|giga\.place)/i;

export const isAllowedRetiredDomainReference = (file, line) =>
	/^\s*Copyright(?: \(c\))? \d{4} dao\.xyz\s*$/.test(line);

export const findUnsafeRetiredDomainReferences = (file, contents) => {
	if (validatorFiles.has(file)) return [];

	const findings = new Set(
		retiredDomainPattern.test(file) ? [`${file}: retired domain in path`] : [],
	);
	const variants = contents.includes("\0")
		? [contents.replaceAll("\0", "")]
		: [contents];
	for (const variant of variants) {
		for (const [index, line] of variant.split(/\r?\n/).entries()) {
			if (!retiredDomainPattern.test(line)) continue;
			if (isAllowedRetiredDomainReference(file, line)) continue;
			findings.add(`${file}:${index + 1}:${line.trim()}`);
		}
	}
	return [...findings];
};

const readTarText = (archive, start, length) =>
	archive
		.subarray(start, start + length)
		.toString("utf8")
		.replace(/\0.*$/s, "")
		.trim();

export const findUnsafeTarArchiveReferences = (file, archive) => {
	const findings = [...findUnsafeRetiredDomainReferences(file, "")];
	let offset = 0;
	while (offset + 512 <= archive.length) {
		const header = archive.subarray(offset, offset + 512);
		if (header.every((byte) => byte === 0)) break;

		const name = readTarText(header, 0, 100);
		const prefix = readTarText(header, 345, 155);
		const entryPath = prefix ? `${prefix}/${name}` : name;
		const sizeText = readTarText(header, 124, 12);
		const size = sizeText ? Number.parseInt(sizeText, 8) : 0;
		if (!Number.isSafeInteger(size) || size < 0) {
			throw new Error(`${file}: invalid tar entry size for ${entryPath}`);
		}

		const contentStart = offset + 512;
		const contentEnd = contentStart + size;
		if (contentEnd > archive.length) {
			throw new Error(`${file}: truncated tar entry ${entryPath}`);
		}
		const entryLabel = `${file}!/${entryPath || `entry-${offset}`}`;
		findings.push(
			...findUnsafeRetiredDomainReferences(
				entryLabel,
				archive.subarray(contentStart, contentEnd).toString("utf8"),
			),
		);

		const linkName = readTarText(header, 157, 100);
		if (linkName) {
			findings.push(
				...findUnsafeRetiredDomainReferences(
					`${entryLabel} -> ${linkName}`,
					"",
				),
			);
		}

		offset = contentStart + Math.ceil(size / 512) * 512;
	}
	return [...new Set(findings)];
};

const scanFile = (file) => {
	const absolutePath = path.join(repoRoot, file);
	if (!existsSync(absolutePath)) return [];
	const contents = readFileSync(absolutePath);
	return file.endsWith(".tgz")
		? findUnsafeTarArchiveReferences(file, gunzipSync(contents))
		: findUnsafeRetiredDomainReferences(file, contents.toString("utf8"));
};

const main = () => {
	const files = execFileSync(
		"git",
		["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
		{
			cwd: repoRoot,
			encoding: "utf8",
		},
	)
		.split("\0")
		.filter(Boolean);
	const findings = files.flatMap(scanFile);

	if (findings.length > 0) {
		console.error(
			`Retired, unowned domains may not be used as active references:\n${findings.join("\n")}`,
		);
		process.exitCode = 1;
		return;
	}
	console.log(
		"Validated that retired domains appear only in exact historical copyright notices and validator fixtures",
	);
};

if (
	process.argv[1] &&
	path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
	main();
}
