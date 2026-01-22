import { useEffect, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import rehypeRaw from "rehype-raw";
import remarkGfm from "remark-gfm";

import { CodeInclude } from "./MarkdownCodeInclude";
import { InternalLink } from "./MarkdownLink";
import { MarkdownImage } from "./MarkdownImage";
import { splitPathDirname } from "../utils/path";

export function Markdown({ base, docPath }: { base: string; docPath: string }) {
	const [content, setContent] = useState<string | null>(null);
	const [error, setError] = useState<string | null>(null);

	const docUrl = useMemo(() => `/${base}/${docPath}`, [base, docPath]);
	const baseDir = useMemo(() => splitPathDirname(docPath), [docPath]);

	useEffect(() => {
		(async () => {
			setError(null);
			setContent(null);
			try {
				const candidates = [docPath];
				if (docPath.endsWith(".md") && !docPath.endsWith("/README.md")) {
					candidates.push(docPath.replace(/\.md$/, "/README.md"));
				}

				let lastError: string | null = null;
				for (const candidate of candidates) {
					const res = await fetch(`/${base}/${candidate}`, { cache: "no-store" });
					if (res.ok) {
						setContent(await res.text());
						return;
					}
					lastError = `HTTP ${res.status}`;
				}

				throw new Error(lastError ?? "Unknown error");
			} catch (e) {
				setError(e instanceof Error ? e.message : String(e));
			}
		})();
	}, [base, docPath, docUrl]);

	if (error) {
		return (
			<div className="mx-auto max-w-3xl">
				<h1 className="text-2xl font-bold">Not found</h1>
				<p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
					Failed to load <code>{docPath}</code>: {error}
				</p>
			</div>
		);
	}

	if (content === null) {
		return (
			<div className="mx-auto max-w-3xl">
				<p className="text-sm text-slate-500">Loading…</p>
			</div>
		);
	}

	return (
		<div className="mx-auto max-w-3xl">
			<article className="prose prose-slate max-w-none dark:prose-invert">
				<ReactMarkdown
					remarkPlugins={[remarkGfm]}
					rehypePlugins={[rehypeRaw]}
					components={{
						script: () => null,
						style: () => null,
						a: ({ href = "", title, children }) => {
							if (typeof title === "string" && title.includes(":include")) {
								return (
									<CodeInclude
										base={base}
										markdownDir={baseDir}
										href={href}
										title={title}
									/>
								);
							}
							return (
								<InternalLink href={href} title={title} docDir={baseDir}>
									{children}
								</InternalLink>
							);
						},
						img: ({ src = "", alt = "" }) => (
							<MarkdownImage base={base} markdownDir={baseDir} src={src} alt={alt} />
						),
						code: ({ className, children }) => {
							return (
								<code className={className}>
									{typeof children === "string" ? children : String(children)}
								</code>
							);
						},
						pre: ({ children }) => (
							<pre className="overflow-x-auto rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm dark:border-slate-800 dark:bg-slate-900/40">
								{children}
							</pre>
						),
					}}
				>
					{content}
				</ReactMarkdown>
			</article>
		</div>
	);
}
