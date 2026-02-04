import { isValidElement, useEffect, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import type { Components } from "react-markdown";
import rehypeRaw from "rehype-raw";
import remarkGfm from "remark-gfm";

import { CodeInclude } from "./MarkdownCodeInclude";
import { InternalLink } from "./MarkdownLink";
import { MarkdownImage } from "./MarkdownImage";
import { highlightToHtml, languageFromClassName } from "../utils/highlight";
import { splitPathDirname } from "../utils/path";
import { FanoutProtocolSandbox } from "./FanoutProtocolSandbox";

export function Markdown({ base, docPath }: { base: string; docPath: string }) {
	const [content, setContent] = useState<string | null>(null);
	const [error, setError] = useState<string | null>(null);
	const [loadedDocPath, setLoadedDocPath] = useState<string | null>(null);

	const docUrl = useMemo(() => `${base}/${docPath}`, [base, docPath]);
	const baseDir = useMemo(
		() => splitPathDirname(loadedDocPath ?? docPath),
		[docPath, loadedDocPath],
	);

	const components = useMemo(() => {
		const out: Components = {
			script: () => null,
			style: () => null,
			a: ({ href = "", title, children, className }) => {
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
					<InternalLink href={href} title={title} docDir={baseDir} className={className}>
						{children}
					</InternalLink>
				);
			},
			img: ({ src = "", alt = "" }) => (
				<MarkdownImage base={base} markdownDir={baseDir} src={src} alt={alt} />
			),
			code: ({ className, children }) => <code className={className}>{children}</code>,
			pre: ({ children }) => {
				const childArray = Array.isArray(children) ? children : [children];
				const codeElement = childArray.find((child) => {
					if (!isValidElement(child)) return false;
					const node = (child.props as { node?: unknown }).node as { tagName?: string } | undefined;
					return child.type === "code" || node?.tagName === "code";
				});

				if (isValidElement(codeElement)) {
					const className = (codeElement.props as { className?: string }).className;
					const lang = languageFromClassName(className);
					const codeChildren = (codeElement.props as { children?: unknown }).children;
					const text =
						typeof codeChildren === "string"
							? codeChildren
							: Array.isArray(codeChildren)
								? codeChildren.join("")
								: String(codeChildren ?? "");
					const html = highlightToHtml(text, lang);
					return (
						<pre className="overflow-x-auto rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm dark:border-slate-800 dark:bg-slate-900/40">
							<code
								className={["hljs", className].filter(Boolean).join(" ")}
								dangerouslySetInnerHTML={{ __html: html }}
							/>
						</pre>
					);
				}

				return (
					<pre className="overflow-x-auto rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm dark:border-slate-800 dark:bg-slate-900/40">
						{children}
					</pre>
				);
			},
		};

		// react-markdown's `Components` type only includes standard HTML tag names.
		// We intentionally support a few custom tags for interactive docs.
		(out as any)["fanout-protocol-sandbox"] = (props: any) => <FanoutProtocolSandbox {...props} />;

		return out;
	}, [base, baseDir]);

	useEffect(() => {
		(async () => {
			setError(null);
			setContent(null);
			setLoadedDocPath(null);
			try {
				const candidates = [docPath];
				if (docPath.endsWith(".md") && !docPath.endsWith("/README.md")) {
					candidates.push(docPath.replace(/\.md$/, "/README.md"));
				}

				let lastError: string | null = null;
				for (const candidate of candidates) {
					const res = await fetch(`${base}/${candidate}`, { cache: "no-store" });
					if (res.ok) {
						setLoadedDocPath(candidate);
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
					components={components}
				>
					{content}
				</ReactMarkdown>
			</article>
		</div>
	);
}
