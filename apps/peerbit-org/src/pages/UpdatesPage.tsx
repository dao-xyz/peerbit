import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { DocsLayout } from "../layout/DocsLayout";

type UpdateKind = "post" | "release";

type UpdatesIndexItem = {
	kind: UpdateKind;
	title: string;
	date: string; // YYYY-MM-DD
	href: string;
	excerpt?: string;
};

const articles = [
	{
		title: "Resource-aware sharding",
		href: "/topics/sharding/sharding.md",
		description: "How Peerbit partitions and routes data with resource constraints.",
	},
	{
		title: "Generalized content space",
		href: "/topics/custom-domain/",
		description: "Build application-specific content addressing on top of Peerbit.",
	},
	{
		title: "SQLite integration",
		href: "/topics/sqlite-integration/",
		description: "Using SQLite as a backend for Peerbit programs.",
	},
	{
		title: "Wallet integration",
		href: "/topics/wallet-integration/",
		description: "Connect identities and permissions to wallets.",
	},
	{
		title: "Zero-knowledge proofs",
		href: "/topics/zero-knowledge/",
		description: "Where ZK can fit in Peerbit-based applications.",
	},
	{
		title: "Forward secrecy",
		href: "/topics/forward-secrecy/",
		description: "Current implementation, tradeoffs, and comparison with Signal.",
	},
	{
		title: "Data integrity",
		href: "/topics/integrity/integrity.md",
		description: "Uniqueness, integrity, and validating content in distributed systems.",
	},
	{
		title: "Peerbit vs [?]",
		href: "/topics/difference/difference.md",
		description: "How Peerbit compares to other approaches and systems.",
	},
] as const;

function formatDate(isoDate: string) {
	const date = new Date(`${isoDate}T00:00:00Z`);
	if (Number.isNaN(date.getTime())) return isoDate;
	return date.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
}

function pill(kind: UpdateKind) {
	return kind === "release" ? "Release" : "Post";
}

export function UpdatesPage() {
	const [items, setItems] = useState<UpdatesIndexItem[] | null>(null);
	const [error, setError] = useState<string | null>(null);
	const [filter, setFilter] = useState<UpdateKind | "all">("all");

	useEffect(() => {
		(async () => {
			setError(null);
			setItems(null);
			try {
				const res = await fetch("/content/docs/updates/index.json", { cache: "no-store" });
				if (!res.ok) throw new Error(`HTTP ${res.status}`);
				setItems((await res.json()) as UpdatesIndexItem[]);
			} catch (e) {
				setError(e instanceof Error ? e.message : String(e));
			}
		})();
	}, []);

	const filtered = useMemo(() => {
		if (!items) return null;
		if (filter === "all") return items;
		return items.filter((i) => i.kind === filter);
	}, [filter, items]);

	const emailFormAction = import.meta.env.VITE_UPDATES_EMAIL_FORM_ACTION as string | undefined;

	return (
		<DocsLayout>
			<div className="mx-auto max-w-3xl">
				<header className="mb-10">
					<h1 className="text-3xl font-extrabold tracking-tight">Updates</h1>
					<p className="mt-2 text-slate-600 dark:text-slate-300">
						Product updates, release announcements, and engineering notes.
					</p>

					<div className="mt-6 grid gap-3 rounded-xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950">
						<div className="flex flex-wrap items-center gap-3 text-sm">
							<span className="font-semibold">Subscribe</span>
							<a
								className="rounded-full bg-slate-100 px-3 py-1 font-semibold hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800"
								href="/content/docs/updates/all.xml"
								target="_blank"
								rel="noreferrer"
							>
								RSS (All)
							</a>
							<a
								className="rounded-full bg-slate-100 px-3 py-1 font-semibold hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800"
								href="/content/docs/updates/posts.xml"
								target="_blank"
								rel="noreferrer"
							>
								RSS (Posts)
							</a>
							<a
								className="rounded-full bg-slate-100 px-3 py-1 font-semibold hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800"
								href="/content/docs/updates/releases.xml"
								target="_blank"
								rel="noreferrer"
							>
								RSS (Releases)
							</a>
						</div>

						<div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
							<div className="text-sm text-slate-600 dark:text-slate-300">
								Get updates by email:
							</div>
							<form
								className="flex w-full gap-2 sm:w-auto"
								action={emailFormAction}
								method="post"
								target="_blank"
							>
								<input
									className="w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-slate-300 dark:border-slate-800 dark:bg-slate-950 dark:focus:ring-slate-700 sm:w-72"
									type="email"
									name="email"
									placeholder="you@example.com"
									required
									disabled={!emailFormAction}
								/>
								<button
									className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:cursor-not-allowed disabled:opacity-50 dark:bg-slate-50 dark:text-slate-950"
									type="submit"
									disabled={!emailFormAction}
									title={
										emailFormAction
											? "Subscribe"
											: "Set VITE_UPDATES_EMAIL_FORM_ACTION to enable email signup"
									}
								>
									Subscribe
								</button>
							</form>
						</div>
					</div>
				</header>

				<section>
					<div className="flex flex-wrap items-center justify-between gap-3">
						<h2 className="text-xl font-bold">Posts</h2>
						<div className="flex gap-2 text-sm">
							<button
								type="button"
								onClick={() => setFilter("all")}
								className={[
									"rounded-full px-3 py-1 font-semibold",
									filter === "all"
										? "bg-slate-900 text-white dark:bg-slate-50 dark:text-slate-950"
										: "bg-slate-100 hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800",
								].join(" ")}
							>
								All
							</button>
							<button
								type="button"
								onClick={() => setFilter("post")}
								className={[
									"rounded-full px-3 py-1 font-semibold",
									filter === "post"
										? "bg-slate-900 text-white dark:bg-slate-50 dark:text-slate-950"
										: "bg-slate-100 hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800",
								].join(" ")}
							>
								Posts
							</button>
							<button
								type="button"
								onClick={() => setFilter("release")}
								className={[
									"rounded-full px-3 py-1 font-semibold",
									filter === "release"
										? "bg-slate-900 text-white dark:bg-slate-50 dark:text-slate-950"
										: "bg-slate-100 hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800",
								].join(" ")}
							>
								Releases
							</button>
						</div>
					</div>

					{error ? (
						<div className="mt-4 rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-800 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-200">
							Failed to load updates: {error}
						</div>
					) : null}

					{filtered === null ? (
						<div className="mt-4 text-sm text-slate-500">Loading…</div>
					) : (
						<div className="mt-4 grid gap-3">
							{filtered.map((item) => (
								<Link
									key={`${item.kind}:${item.href}`}
									to={item.href}
									className="group block rounded-xl border border-slate-200 bg-white p-4 no-underline shadow-sm transition hover:shadow-md dark:border-slate-800 dark:bg-slate-950"
								>
									<div className="flex items-start justify-between gap-3">
										<div>
											<div className="font-semibold text-slate-900 dark:text-slate-50">
												{item.title}
											</div>
											<div className="mt-1 text-xs text-slate-500 dark:text-slate-400">
												{formatDate(item.date)}
											</div>
										</div>
										<span className="text-xs text-slate-500 dark:text-slate-400">
											{pill(item.kind)}
										</span>
									</div>
									{item.excerpt ? (
										<p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
											{item.excerpt}
										</p>
									) : null}
								</Link>
							))}
							{filtered.length === 0 ? (
								<div className="text-sm text-slate-500">No updates yet.</div>
							) : null}
						</div>
					)}
				</section>

				<section className="mt-12">
					<h2 className="text-xl font-bold">Articles</h2>
					<div className="mt-4 grid gap-3 sm:grid-cols-2">
						{articles.map((a) => (
							<Link
								key={a.href}
								to={a.href}
								className="group block rounded-xl border border-slate-200 bg-white p-4 no-underline shadow-sm transition hover:shadow-md dark:border-slate-800 dark:bg-slate-950"
							>
								<div className="flex items-center justify-between gap-3">
									<span className="font-semibold text-slate-900 dark:text-slate-50">
										{a.title}
									</span>
									<span className="text-xs text-slate-500 dark:text-slate-400">Article</span>
								</div>
								<p className="mt-2 text-sm text-slate-600 dark:text-slate-300">{a.description}</p>
							</Link>
						))}
					</div>
				</section>
			</div>
		</DocsLayout>
	);
}

