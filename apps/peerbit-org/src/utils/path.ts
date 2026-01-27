export function splitPathDirname(path: string) {
	const normalized = path.replace(/\\/g, "/");
	const parts = normalized.split("/").filter(Boolean);
	parts.pop();
	return parts.length ? `${parts.join("/")}/` : "";
}

export function resolveRelativePath(baseDir: string, href: string) {
	if (/^https?:\/\//.test(href)) return href;

	const baseUrl = new URL(baseDir, "https://peerbit.local/");
	const resolved = new URL(href, baseUrl);
	return resolved.pathname.replace(/^\//, "");
}

export function joinContentPaths(baseDir: string, resolvedPath: string) {
	if (resolvedPath.startsWith("/")) return resolvedPath.slice(1);
	if (!baseDir) return resolvedPath;
	return `${baseDir}${resolvedPath}`.replace(/\/{2,}/g, "/");
}

