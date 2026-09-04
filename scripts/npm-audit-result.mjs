const isRecord = (value) =>
	value !== null && typeof value === "object" && !Array.isArray(value);

const asText = (value) => {
	if (typeof value === "string") return value;
	if (value === undefined || value === null) return "";
	return value.toString("utf8");
};

const transientErrorCodes = new Set([
	"EAI_AGAIN",
	"ECONNREFUSED",
	"ECONNRESET",
	"EHOSTUNREACH",
	"ENETUNREACH",
	"ENOTFOUND",
	"ERR_SOCKET_TIMEOUT",
	"ESOCKETTIMEDOUT",
	"ETIMEDOUT",
]);

const endpointPattern =
	/(?:\/-\/npm\/v1\/security\/(?:advisories\/bulk|audits\/quick)|\b(?:advisory|audit) (?:endpoint|request|service)\b)/i;
const transientNetworkCodePattern =
	/\b(?:EAI_AGAIN|ECONNREFUSED|ECONNRESET|EHOSTUNREACH|ENETUNREACH|ENOTFOUND|ERR_SOCKET_TIMEOUT|ESOCKETTIMEDOUT|ETIMEDOUT)\b/i;
const transientMessagePattern =
	/(?:\bnetwork timeout\b|\brequest timed out\b|\bsocket (?:hang up|timeout)\b|\btoo many requests\b|\bservice unavailable\b|\bbad gateway\b|\bgateway timeout\b)/i;
const transientHttpPattern =
	/(?:\bE(?:429|5\d\d)\b|\bHTTP(?: status)?\s*[:=]?\s*(?:429|5\d\d)\b|\bstatus(?: code)?\s*[:=]\s*(?:429|5\d\d)\b|\b(?:429|5\d\d)\s+(?:GET|POST)\b)/i;
const clientHttpPattern =
	/(?:\bE4(?!29)\d\d\b|\bHTTP(?: status)?\s*[:=]?\s*4(?!29)\d\d\b|\bstatus(?: code)?\s*[:=]\s*4(?!29)\d\d\b)/i;
const authenticationPattern =
	/(?:\bENEEDAUTH\b|\bunauthori[sz]ed\b|\bforbidden\b|\bauthentication required\b|\binvalid (?:auth|configuration|registry|token)\b)/i;
const severityNames = ["info", "low", "moderate", "high", "critical"];

const isTransientAuditEndpointResponse = (report) => {
	if (!isRecord(report)) return false;
	const statusCode = report.statusCode;
	return (
		typeof report.uri === "string" &&
		endpointPattern.test(report.uri) &&
		typeof report.method === "string" &&
		report.method.toUpperCase() === "POST" &&
		Number.isSafeInteger(statusCode) &&
		(statusCode === 408 ||
			statusCode === 425 ||
			statusCode === 429 ||
			(statusCode >= 500 && statusCode <= 599))
	);
};

const result = (outcome, reason, report) =>
	report === undefined ? { outcome, reason } : { outcome, reason, report };

/**
 * Classifies the result of a spawned `npm audit --json` process without making
 * any network request. The input accepts the status/error/stdout/stderr fields
 * returned by `spawnSync`.
 *
 * A structurally valid positive report is authoritative even when npm exits in
 * an unexpected way. Only recognized transient failures of the advisory
 * endpoint are soft; malformed output and local/configuration failures remain
 * hard failures so the scanner cannot be disabled silently.
 */
export const classifyNpmAuditResult = (audit) => {
	const stdout = asText(audit?.stdout).trim();
	const stderr = asText(audit?.stderr).trim();
	const errorCode =
		typeof audit?.error?.code === "string"
			? audit.error.code.toUpperCase()
			: undefined;
	const errorMessage =
		typeof audit?.error?.message === "string" ? audit.error.message : "";
	const diagnostic = [stdout, stderr, errorCode, errorMessage]
		.filter(Boolean)
		.join("\n");

	let report;
	if (stdout !== "") {
		try {
			report = JSON.parse(stdout);
		} catch {
			// A transient endpoint failure can return a non-JSON proxy response. It
			// is classified from the bounded diagnostic below.
		}
	}

	if (isRecord(report) && report.auditReportVersion === 2) {
		const vulnerabilities = report.vulnerabilities;
		const vulnerabilityMetadata = report.metadata?.vulnerabilities;
		const total = vulnerabilityMetadata?.total;
		const severityCounts = severityNames.map(
			(severity) => vulnerabilityMetadata?.[severity],
		);
		if (
			!isRecord(vulnerabilities) ||
			!isRecord(vulnerabilityMetadata) ||
			!Number.isSafeInteger(total) ||
			total < 0 ||
			severityCounts.some(
				(count) => !Number.isSafeInteger(count) || count < 0,
			) ||
			severityCounts.reduce((sum, count) => sum + count, 0) !== total
		) {
			return result(
				"invalid",
				"npm audit returned a malformed v2 report",
				report,
			);
		}

		const vulnerabilityNodes = Object.keys(vulnerabilities).length;
		if (total > 0 && vulnerabilityNodes > 0) {
			return result(
				"findings",
				`npm audit reported ${total} production vulnerability finding${total === 1 ? "" : "s"}`,
				report,
			);
		}
		if ((total === 0) !== (vulnerabilityNodes === 0)) {
			return result(
				"invalid",
				"npm audit returned inconsistent vulnerability totals",
				report,
			);
		}

		if (errorCode === "ETIMEDOUT") {
			return result(
				"unavailable",
				"npm audit exceeded its bounded process timeout",
				report,
			);
		}
		if (
			audit?.status !== 0 ||
			(audit?.signal !== null && audit?.signal !== undefined) ||
			audit?.error !== undefined
		) {
			return result(
				"invalid",
				"npm audit returned a zero-finding report with a failing process status",
				report,
			);
		}
		return result("clean", "npm audit reported no production findings", report);
	}

	if (errorCode === "ENOENT") {
		return result("invalid", "the npm audit executable could not be started");
	}
	if (isTransientAuditEndpointResponse(report)) {
		return result(
			"unavailable",
			`the npm advisory endpoint returned temporary HTTP status ${report.statusCode}`,
		);
	}
	if (
		clientHttpPattern.test(diagnostic) ||
		authenticationPattern.test(diagnostic)
	) {
		return result(
			"invalid",
			"npm audit failed because of an authentication or request configuration error",
		);
	}
	if (errorCode === "ETIMEDOUT") {
		return result(
			"unavailable",
			"npm audit exceeded its bounded process timeout",
		);
	}

	const endpointFailure =
		endpointPattern.test(diagnostic) &&
		(transientErrorCodes.has(errorCode) ||
			transientNetworkCodePattern.test(diagnostic) ||
			transientMessagePattern.test(diagnostic) ||
			transientHttpPattern.test(diagnostic));
	const endpointServiceFailure =
		transientMessagePattern.test(diagnostic) &&
		transientHttpPattern.test(diagnostic);
	if (endpointFailure || endpointServiceFailure) {
		return result(
			"unavailable",
			"the npm advisory endpoint was temporarily unavailable",
		);
	}

	return result("invalid", "npm audit did not return a valid v2 report");
};
