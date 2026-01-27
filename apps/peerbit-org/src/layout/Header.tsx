import { NavLink } from "react-router-dom";
import type { ReactNode } from "react";

import { Container } from "../ui/Container";

function HeaderLink({ to, children }: { to: string; children: ReactNode }) {
	return (
		<NavLink
			to={to}
			className={({ isActive }) =>
				[
					"rounded-full px-3 py-1 text-sm font-semibold transition",
					"hover:bg-slate-100 dark:hover:bg-slate-900",
					isActive ? "bg-slate-100 dark:bg-slate-900" : "",
				].join(" ")
			}
		>
			{children}
		</NavLink>
	);
}

export function Header() {
	return (
		<div className="sticky top-0 z-50 border-b border-slate-200 bg-white/70 backdrop-blur dark:border-slate-800 dark:bg-slate-950/70">
			<Container className="flex h-14 items-center justify-between">
				<div className="flex items-center gap-3">
					<a href="#/" className="flex items-center gap-2 font-bold">
						<img src="/content/docs/peerbit-logo.png" alt="Peerbit" className="h-7 w-7" />
						<span>Peerbit</span>
					</a>
					<span className="hidden text-xs text-slate-500 sm:inline dark:text-slate-400">
						P2P framework
					</span>
				</div>
				<nav className="flex items-center gap-1">
					<HeaderLink to="/docs/getting-started">Docs</HeaderLink>
					<HeaderLink to="/blog">Blog</HeaderLink>
					<HeaderLink to="/release-notes">Release notes</HeaderLink>
					<HeaderLink to="/status">Status</HeaderLink>
					<a
						className="ml-1 rounded-full px-3 py-1 text-sm font-semibold transition hover:bg-slate-100 dark:hover:bg-slate-900"
						href="https://github.com/dao-xyz/peerbit"
						target="_blank"
						rel="noreferrer"
					>
						GitHub
					</a>
				</nav>
			</Container>
		</div>
	);
}
