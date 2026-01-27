import { useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { InternalLink } from "../ui/MarkdownLink";

export function DocsSidebar() {
	const [content, setContent] = useState<string | null>(null);

	useEffect(() => {
		(async () => {
			try {
				const res = await fetch("/content/docs/_sidebar.md", { cache: "no-store" });
				if (!res.ok) throw new Error(`HTTP ${res.status}`);
				setContent(await res.text());
			} catch {
				setContent(null);
			}
		})();
	}, []);

	if (!content) {
		return (
			<div className="text-sm text-slate-500">
				Loading navigation…
			</div>
		);
	}

	return (
		<nav className="text-sm">
			<ReactMarkdown
				remarkPlugins={[remarkGfm]}
				components={{
					p: ({ children }) => (
						<div className="mt-6 mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
							{children}
						</div>
					),
					ul: ({ children }) => <ul className="space-y-1">{children}</ul>,
					li: ({ children }) => <li className="ml-0">{children}</li>,
					a: ({ href = "", title, children }) => (
						<InternalLink href={href} title={title} docDir="">
							<span className="block rounded-md px-2 py-1 hover:bg-slate-100 dark:hover:bg-slate-900">
								{children}
							</span>
						</InternalLink>
					),
				}}
			>
				{content}
			</ReactMarkdown>
		</nav>
	);
}
