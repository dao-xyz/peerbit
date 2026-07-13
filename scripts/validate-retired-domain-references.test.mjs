import assert from "node:assert/strict";
import test from "node:test";
import {
	findUnsafeRetiredDomainReferences,
	findUnsafeTarArchiveReferences,
	isAllowedRetiredDomainReference,
} from "./validate-retired-domain-references.mjs";

test("allows only exact historical copyright attribution", () => {
	assert.equal(
		isAllowedRetiredDomainReference(
			"packages/example/package.json",
			'    "author": "dao.xyz",',
		),
		false,
	);
	assert.equal(
		isAllowedRetiredDomainReference(
			"packages/example/LICENSE",
			"Copyright (c) 2022 dao.xyz",
		),
		true,
	);
});

test("rejects active URLs, DNS names, and maintainer values", () => {
	const findings = findUnsafeRetiredDomainReferences(
		"packages/example/config.ts",
		[
			'const api = "https://files.dao.xyz";',
			'const fallback = "https://giga.place";',
			'const maintainers = ["dao.xyz"];',
		].join("\n"),
	);
	assert.equal(findings.length, 3);
});

test("does not allow active references hidden in attribution-like lines", () => {
	assert.equal(
		isAllowedRetiredDomainReference(
			"packages/example/not-package.json",
			'"author": "dao.xyz"',
		),
		false,
	);
	assert.equal(
		isAllowedRetiredDomainReference(
			"packages/example/LICENSE",
			"Copyright (c) 2022 dao.xyz; endpoint=https://files.dao.xyz",
		),
		false,
	);
});

test("detects UTF-16-style NUL-separated domain text", () => {
	const encoded = [..."https://files.dao.xyz"]
		.map((character) => `${character}\0`)
		.join("");
	assert.equal(
		findUnsafeRetiredDomainReferences("packages/example/fixture.bin", encoded)
			.length,
		1,
	);
});

test("detects retired domains used only in tracked paths", () => {
	assert.equal(
		findUnsafeRetiredDomainReferences(
			"config/files.dao.xyz/certificate.pem",
			"",
		).length,
		1,
	);
});

test("inspects text stored inside tracked tar archives", () => {
	const contents = Buffer.from(
		'{"author":"Peerbit contributors","endpoint":"https://files.dao.xyz"}',
	);
	const header = Buffer.alloc(512);
	header.write("package/package.json", 0, "utf8");
	header.write(
		contents.length.toString(8).padStart(11, "0") + "\0",
		124,
		"ascii",
	);
	header[156] = "0".charCodeAt(0);
	const padding = Buffer.alloc(
		Math.ceil(contents.length / 512) * 512 - contents.length,
	);
	const archive = Buffer.concat([
		header,
		contents,
		padding,
		Buffer.alloc(1024),
	]);

	assert.equal(
		findUnsafeTarArchiveReferences("fixtures/package.tgz", archive).length,
		1,
	);
});
