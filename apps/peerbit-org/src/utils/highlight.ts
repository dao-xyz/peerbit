import hljs from "highlight.js";

function escapeHtml(source: string) {
	return source
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;")
		.replace(/'/g, "&#039;");
}

export function languageFromClassName(className: string | undefined) {
	const match = className?.match(/language-([A-Za-z0-9_-]+)/);
	return match?.[1]?.toLowerCase() ?? null;
}

const extensionToLanguage: Record<string, string> = {
	ts: "typescript",
	tsx: "tsx",
	js: "javascript",
	jsx: "jsx",
	mjs: "javascript",
	cjs: "javascript",
	json: "json",
	md: "markdown",
	sh: "bash",
	bash: "bash",
	yml: "yaml",
	yaml: "yaml",
	html: "xml",
	xml: "xml",
	css: "css",
};

export function languageFromPath(path: string) {
	const normalized = path.split("?")[0]?.split("#")[0] ?? "";
	const match = normalized.match(/\.([A-Za-z0-9]+)$/);
	const ext = match?.[1]?.toLowerCase();
	if (!ext) return null;
	return extensionToLanguage[ext] ?? ext;
}

function highlightWithLanguage(code: string, language: string) {
	const api = hljs as unknown as {
		getLanguage?: (name: string) => unknown;
		highlight?: (...args: unknown[]) => { value?: string };
	};

	if (api.getLanguage && !api.getLanguage(language)) return null;
	if (!api.highlight) return null;

	try {
		// highlight.js v11: highlight(code, { language, ignoreIllegals? })
		return api.highlight(code, { language, ignoreIllegals: true }).value ?? null;
	} catch {
		return null;
	}
}

export function highlightToHtml(code: string, language: string | null) {
	const normalized = code.replace(/\n$/, "");
	if (!normalized) return "";

	const languageHtml = language ? highlightWithLanguage(normalized, language) : null;
	if (languageHtml) return languageHtml;

	try {
		const res = hljs.highlightAuto(normalized);
		return res.value || escapeHtml(normalized);
	} catch {
		return escapeHtml(normalized);
	}
}
