import { NavLink, useLocation } from "react-router-dom";
import { useEffect, useMemo, useState } from "react";
import { ChatLines, Github, HalfMoon, Menu, SunLight, Xmark } from "iconoir-react";

import { Container } from "../ui/Container";
import { IconButton } from "../ui/IconButton";
import { IconLink } from "../ui/IconLink";
import { getActiveTheme, setTheme, type Theme } from "../utils/theme";

function HeaderLink({ to, children }: { to: string; children: string }) {
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

function MobileLink({
	to,
	children,
	onNavigate,
}: {
	to: string;
	children: string;
	onNavigate: () => void;
}) {
	return (
		<NavLink
			to={to}
			onClick={onNavigate}
			className={({ isActive }) =>
				[
					"block rounded-lg px-3 py-2 text-base font-semibold transition",
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
	const location = useLocation();

	const [theme, setThemeState] = useState<Theme>(() => getActiveTheme());
	const [mobileOpen, setMobileOpen] = useState(false);

	const toggleTheme = () => {
		const next: Theme = theme === "dark" ? "light" : "dark";
		setTheme(next);
		setThemeState(next);
	};

	useEffect(() => {
		setMobileOpen(false);
	}, [location.pathname]);

	useEffect(() => {
		if (!mobileOpen) return;
		const previous = document.body.style.overflow;
		document.body.style.overflow = "hidden";
		return () => {
			document.body.style.overflow = previous;
		};
	}, [mobileOpen]);

	useEffect(() => {
		if (!mobileOpen) return;
		const onKeyDown = (e: KeyboardEvent) => {
			if (e.key === "Escape") setMobileOpen(false);
		};
		window.addEventListener("keydown", onKeyDown);
		return () => window.removeEventListener("keydown", onKeyDown);
	}, [mobileOpen]);

	const themeLabel = theme === "dark" ? "Switch to light mode" : "Switch to dark mode";
	const themeIcon = useMemo(
		() =>
			theme === "dark" ? (
				<SunLight className="h-5 w-5" />
			) : (
				<HalfMoon className="h-5 w-5" />
			),
		[theme],
	);

	return (
		<>
			<div className="sticky top-0 z-50 border-b border-slate-200 bg-white/70 backdrop-blur dark:border-slate-800 dark:bg-slate-950/70">
				<Container className="flex h-14 items-center justify-between">
					<div className="flex items-center gap-3">
							<a href="#/" className="flex items-center gap-2 font-bold">
								<img
									src="content/docs/peerbit-logo.png"
									alt="Peerbit"
									className="h-7 w-7"
								/>
								<span>Peerbit</span>
						</a>
						<span className="hidden text-xs text-slate-500 sm:inline dark:text-slate-400">
							P2P framework
						</span>
					</div>

					<nav className="hidden items-center gap-1 md:flex">
						<HeaderLink to="/docs/getting-started">Docs</HeaderLink>
						<HeaderLink to="/updates">Updates</HeaderLink>
						<HeaderLink to="/release-notes">Release notes</HeaderLink>
						<HeaderLink to="/status">Status</HeaderLink>

						<a
							className="ml-2 inline-flex items-center gap-2 rounded-full bg-slate-900 px-3 py-1 text-sm font-semibold text-white transition hover:bg-slate-800 dark:bg-slate-50 dark:text-slate-950 dark:hover:bg-white"
							href="https://matrix.to/#/#peerbit:matrix.org"
							target="_blank"
							rel="noreferrer"
						>
							<ChatLines className="h-4 w-4" />
							Community chat
						</a>

						<IconLink
							className="ml-1"
							href="https://github.com/dao-xyz/peerbit"
							target="_blank"
							rel="noreferrer"
							aria-label="Peerbit on GitHub"
							title="GitHub"
						>
							<Github className="h-5 w-5" />
						</IconLink>

						<IconButton aria-label={themeLabel} title={themeLabel} onClick={toggleTheme}>
							{themeIcon}
						</IconButton>
					</nav>

					<div className="flex items-center gap-2 md:hidden">
						<a
							className="inline-flex items-center gap-2 rounded-full bg-slate-900 px-3 py-1 text-sm font-semibold text-white transition hover:bg-slate-800 dark:bg-slate-50 dark:text-slate-950 dark:hover:bg-white"
							href="https://matrix.to/#/#peerbit:matrix.org"
							target="_blank"
							rel="noreferrer"
						>
							<ChatLines className="h-4 w-4" />
							Chat
						</a>

						<IconLink
							href="https://github.com/dao-xyz/peerbit"
							target="_blank"
							rel="noreferrer"
							aria-label="Peerbit on GitHub"
							title="GitHub"
						>
							<Github className="h-5 w-5" />
						</IconLink>

						<IconButton aria-label={themeLabel} title={themeLabel} onClick={toggleTheme}>
							{themeIcon}
						</IconButton>

						<IconButton
							aria-label={mobileOpen ? "Close menu" : "Open menu"}
							title={mobileOpen ? "Close menu" : "Open menu"}
							onClick={() => setMobileOpen((v) => !v)}
						>
							{mobileOpen ? <Xmark className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
						</IconButton>
					</div>
				</Container>
			</div>

			{mobileOpen ? (
				<div className="fixed inset-0 z-40 md:hidden">
					<button
						type="button"
						aria-label="Close menu"
						className="absolute inset-0 bg-slate-950/40 backdrop-blur-sm"
						onClick={() => setMobileOpen(false)}
					/>
					<div className="absolute left-0 right-0 top-14 border-b border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-950">
						<Container className="py-4">
							<div className="flex flex-col gap-1">
								<MobileLink to="/docs/getting-started" onNavigate={() => setMobileOpen(false)}>
									Docs
								</MobileLink>
								<MobileLink to="/updates" onNavigate={() => setMobileOpen(false)}>
									Updates
								</MobileLink>
								<MobileLink to="/release-notes" onNavigate={() => setMobileOpen(false)}>
									Release notes
								</MobileLink>
								<MobileLink to="/status" onNavigate={() => setMobileOpen(false)}>
									Status
								</MobileLink>
								<a
									className="mt-2 inline-flex items-center justify-center gap-2 rounded-lg bg-slate-900 px-3 py-2 text-base font-semibold text-white transition hover:bg-slate-800 dark:bg-slate-50 dark:text-slate-950 dark:hover:bg-white"
									href="https://matrix.to/#/#peerbit:matrix.org"
									target="_blank"
									rel="noreferrer"
								>
									<ChatLines className="h-5 w-5" />
									Community chat
								</a>
							</div>
						</Container>
					</div>
				</div>
			) : null}
		</>
	);
}
