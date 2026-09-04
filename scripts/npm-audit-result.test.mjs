import assert from "node:assert/strict";
import test from "node:test";
import { classifyNpmAuditResult } from "./npm-audit-result.mjs";

const auditReport = ({ total, vulnerabilities }) => ({
	auditReportVersion: 2,
	vulnerabilities,
	metadata: {
		vulnerabilities: {
			info: 0,
			low: 0,
			moderate: 0,
			high: total,
			critical: 0,
			total,
		},
	},
});

const cleanReport = auditReport({ total: 0, vulnerabilities: {} });
const findingReport = auditReport({
	total: 1,
	vulnerabilities: {
		"unsafe-package": {
			name: "unsafe-package",
			severity: "high",
		},
	},
});

const classify = ({ status = 0, stdout = "", stderr = "", ...rest }) =>
	classifyNpmAuditResult({ status, stdout, stderr, ...rest });

test("classifies a valid zero-finding v2 report as clean", () => {
	const classified = classify({ stdout: JSON.stringify(cleanReport) });
	assert.equal(classified.outcome, "clean");
	assert.deepEqual(classified.report, cleanReport);
});

for (const status of [1, 0]) {
	test(`classifies a valid finding report as findings at status ${status}`, () => {
		const classified = classify({
			status,
			stdout: JSON.stringify(findingReport),
		});
		assert.equal(classified.outcome, "findings");
		assert.deepEqual(classified.report, findingReport);
	});
}

test("classifies an advisory endpoint timeout response as unavailable", () => {
	const classified = classify({
		status: 1,
		stdout: JSON.stringify({
			message:
				"network timeout at: https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
			error: { summary: "", detail: "" },
		}),
	});
	assert.equal(classified.outcome, "unavailable");
});

test("classifies an endpoint-specific connection reset as unavailable", () => {
	const classified = classify({
		status: 1,
		stderr:
			"npm error ECONNRESET POST https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
	});
	assert.equal(classified.outcome, "unavailable");
});

test("classifies an advisory endpoint 503 as unavailable", () => {
	const classified = classify({
		status: 1,
		stdout: JSON.stringify({ error: "Service Unavailable" }),
		stderr:
			"npm error 503 POST https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
	});
	assert.equal(classified.outcome, "unavailable");
});

test("classifies a structured advisory endpoint 500 as unavailable", () => {
	const classified = classify({
		status: 1,
		stdout: JSON.stringify({
			message:
				"500 Internal Server Error - POST https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
			method: "POST",
			uri: "https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
			statusCode: 500,
		}),
	});
	assert.equal(classified.outcome, "unavailable");
});

test("classifies a structured advisory endpoint 408 as unavailable", () => {
	const classified = classify({
		status: 1,
		stdout: JSON.stringify({
			message:
				"HTTP status: 408 - POST https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
			method: "POST",
			uri: "https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
			statusCode: 408,
		}),
	});
	assert.equal(classified.outcome, "unavailable");
});

test("classifies an advisory endpoint 429 as unavailable", () => {
	const classified = classify({
		status: 1,
		stderr:
			"npm error E429 POST https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
	});
	assert.equal(classified.outcome, "unavailable");
});

test("classifies a bounded spawn timeout as unavailable", () => {
	const error = Object.assign(new Error("spawnSync npm ETIMEDOUT"), {
		code: "ETIMEDOUT",
	});
	const classified = classify({ status: null, error });
	assert.equal(classified.outcome, "unavailable");
});

test("classifies malformed scanner output as invalid", () => {
	const classified = classify({ status: 1, stdout: "{not-json" });
	assert.equal(classified.outcome, "invalid");
});

test("classifies a missing npm executable as invalid", () => {
	const error = Object.assign(new Error("spawnSync npm ENOENT"), {
		code: "ENOENT",
	});
	const classified = classify({ status: null, error });
	assert.equal(classified.outcome, "invalid");
});

test("classifies an audit endpoint 401 as invalid", () => {
	const classified = classify({
		status: 1,
		stderr:
			"npm error E401 POST https://registry.npmjs.org/-/npm/v1/security/advisories/bulk",
	});
	assert.equal(classified.outcome, "invalid");
});

test("classifies a zero-finding report with status 1 as invalid", () => {
	const classified = classify({
		status: 1,
		stdout: JSON.stringify(cleanReport),
	});
	assert.equal(classified.outcome, "invalid");
});

test("classifies inconsistent report totals as invalid", () => {
	const classified = classify({
		status: 1,
		stdout: JSON.stringify(
			auditReport({
				total: 0,
				vulnerabilities: findingReport.vulnerabilities,
			}),
		),
	});
	assert.equal(classified.outcome, "invalid");
});

test("classifies inconsistent severity subtotals as invalid", () => {
	const report = auditReport({ total: 0, vulnerabilities: {} });
	report.metadata.vulnerabilities.high = 1;
	const classified = classify({ stdout: JSON.stringify(report) });
	assert.equal(classified.outcome, "invalid");
});
