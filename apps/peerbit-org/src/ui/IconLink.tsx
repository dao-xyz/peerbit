import type { AnchorHTMLAttributes, ReactNode } from "react";

export function IconLink({
	className = "",
	children,
	...props
}: AnchorHTMLAttributes<HTMLAnchorElement> & { children: ReactNode }) {
	return (
		<a
			className={[
				"inline-flex h-9 w-9 items-center justify-center rounded-full",
				"border border-slate-200 bg-white/70 backdrop-blur",
				"transition hover:bg-slate-100",
				"dark:border-slate-800 dark:bg-slate-950/70 dark:hover:bg-slate-900",
				"focus:outline-none focus:ring-2 focus:ring-slate-400/40 dark:focus:ring-slate-500/40",
				className,
			].join(" ")}
			{...props}
		>
			{children}
		</a>
	);
}

