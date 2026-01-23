import { Link } from "react-router-dom";
import type { ReactNode } from "react";

import { resolveRelativePath } from "../utils/path";

function isExternal(href: string) {
	return /^https?:\/\//.test(href);
}

function isHashLink(href: string) {
	return href.startsWith("#");
}

function toAppRoute(href: string, docDir: string) {
	if (href === "/") return "/";

	const resolved = resolveRelativePath(docDir, href);
	if (resolved.endsWith(".md")) {
		const withoutMd = resolved.replace(/\.md$/, "");
		if (withoutMd.endsWith("/README")) {
			return `/docs/${withoutMd.replace(/\/README$/, "")}`;
		}
		return `/docs/${withoutMd}`;
	}
	return `/docs/${resolved.replace(/\/$/, "")}`;
}

export function InternalLink({
	href,
	title,
	docDir,
	children,
	className,
}: {
	href: string;
	title?: string | null;
	docDir: string;
	children: ReactNode;
	className?: string;
}) {
	if (isExternal(href) || isHashLink(href)) {
		return (
			<a
				href={href}
				title={title ?? undefined}
				className={className}
				target={isExternal(href) ? "_blank" : undefined}
				rel={isExternal(href) ? "noreferrer" : undefined}
			>
				{children}
			</a>
		);
	}

	return (
		<Link to={toAppRoute(href, docDir)} title={title ?? undefined} className={className}>
			{children}
		</Link>
	);
}
