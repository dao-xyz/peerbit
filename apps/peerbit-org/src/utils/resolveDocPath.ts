function stripLeadingSlash(pathname: string) {
	return pathname.replace(/^\//, "");
}

export function resolveDocPathFromLocation(pathname: string) {
	const raw = stripLeadingSlash(pathname);

	if (!raw || raw === "/") return "README.md";

	const withoutDocsPrefix = raw.startsWith("docs/") ? raw.slice("docs/".length) : raw;
	const normalized = withoutDocsPrefix.replace(/\/+$/, "");

	if (!normalized) return "README.md";

	if (normalized.endsWith(".md")) return normalized;

	// Prefer "foo.md", fallback to "foo/README.md" handled by runtime (see Markdown)
	return `${normalized}.md`;
}

