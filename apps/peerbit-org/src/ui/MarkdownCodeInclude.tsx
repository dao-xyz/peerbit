import { useEffect, useMemo, useState } from "react";

import { resolveRelativePath } from "../utils/path";
import { highlightToHtml, languageFromPath } from "../utils/highlight";

function parseIncludeTitle(title: string) {
	const fragmentMatch = title.match(/:fragment=([A-Za-z0-9_-]+)/);
	const langMatch = title.match(/:lang=([A-Za-z0-9_-]+)/);
	return { fragment: fragmentMatch?.[1] ?? null, lang: langMatch?.[1]?.toLowerCase() ?? null };
}

function extractFragment(source: string, fragment: string) {
	const marker = `/// [${fragment}]`;
	const indices: number[] = [];
	let pos = 0;
	while (true) {
		const i = source.indexOf(marker, pos);
		if (i === -1) break;
		indices.push(i);
		pos = i + marker.length;
	}
	if (indices.length < 2) return source;

	const start = indices[0] + marker.length;
	const end = indices[1];
	return source.slice(start, end).replace(/^\s*\n/, "").replace(/\n\s*$/, "");
}

export function CodeInclude({
	base,
	markdownDir,
	href,
	title,
}: {
	base: string;
	markdownDir: string;
	href: string;
	title: string;
}) {
	const { fragment, lang } = useMemo(() => parseIncludeTitle(title), [title]);
	const resolved = useMemo(() => resolveRelativePath(markdownDir, href), [href, markdownDir]);
	const url = useMemo(() => `/${base}/${resolved}`, [base, resolved]);
	const inferredLanguage = useMemo(() => lang ?? languageFromPath(resolved), [lang, resolved]);

	const [code, setCode] = useState<string | null>(null);
	const [error, setError] = useState<string | null>(null);

	useEffect(() => {
		(async () => {
			setError(null);
			setCode(null);
			try {
				if (/^https?:\/\//.test(resolved)) {
					throw new Error("External includes are not supported");
				}
				const res = await fetch(url, { cache: "no-store" });
				if (!res.ok) throw new Error(`HTTP ${res.status}`);
				let text = await res.text();
				if (fragment) text = extractFragment(text, fragment);
				setCode(text.trimEnd());
			} catch (e) {
				setError(e instanceof Error ? e.message : String(e));
			}
		})();
	}, [fragment, url]);

	if (error) {
		return (
			<div className="my-4 rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-800 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-200">
				Failed to include <code>{href}</code>: {error}
			</div>
		);
	}

	if (code === null) {
		return <div className="my-4 text-sm text-slate-500">Loading snippet…</div>;
	}

	const highlighted = highlightToHtml(code, inferredLanguage);

	return (
		<pre className="my-4 overflow-x-auto rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm dark:border-slate-800 dark:bg-slate-900/40">
			<code
				className={["hljs", inferredLanguage ? `language-${inferredLanguage}` : null]
					.filter(Boolean)
					.join(" ")}
				dangerouslySetInnerHTML={{ __html: highlighted }}
			/>
		</pre>
	);
}
