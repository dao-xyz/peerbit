import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { BellNotification, Xmark } from "iconoir-react";

import { DocsLayout } from "../layout/DocsLayout";
import { IconButton } from "../ui/IconButton";

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
	const [subscribeOpen, setSubscribeOpen] = useState(false);
	const [subscribeTo, setSubscribeTo] = useState<UpdateKind | "all">("all");
	const [email, setEmail] = useState("");
	const [subscribeStatus, setSubscribeStatus] = useState<
		{ type: "idle" } | { type: "loading" } | { type: "success"; message: string } | { type: "error"; message: string }
	>({ type: "idle" });

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
	const feedRssUrl = useMemo(() => {
		if (subscribeTo === "post") return "/content/docs/updates/posts.xml";
		if (subscribeTo === "release") return "/content/docs/updates/releases.xml";
		return "/content/docs/updates/all.xml";
	}, [subscribeTo]);
	const feedJsonUrl = useMemo(() => {
		if (subscribeTo === "post") return "/content/docs/updates/posts.json";
		if (subscribeTo === "release") return "/content/docs/updates/releases.json";
		return "/content/docs/updates/all.json";
	}, [subscribeTo]);

	useEffect(() => {
		if (!subscribeOpen) return;

		const previous = document.body.style.overflow;
		document.body.style.overflow = "hidden";

		const onKeyDown = (e: KeyboardEvent) => {
			if (e.key === "Escape") setSubscribeOpen(false);
		};
		window.addEventListener("keydown", onKeyDown);

		return () => {
			document.body.style.overflow = previous;
			window.removeEventListener("keydown", onKeyDown);
		};
	}, [subscribeOpen]);

	useEffect(() => {
		if (!subscribeOpen) setSubscribeStatus({ type: "idle" });
	}, [subscribeOpen]);

	async function onSubscribe(e: FormEvent) {
		e.preventDefault();
		if (!emailFormAction) return;

		setSubscribeStatus({ type: "loading" });
		try {
			const res = await fetch(emailFormAction, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ email, topic: subscribeTo }),
			});

			if (!res.ok) {
				const contentType = res.headers.get("content-type") ?? "";
				if (contentType.includes("application/json")) {
					const json = (await res.json().catch(() => ({}))) as { error?: string };
					setSubscribeStatus({ type: "error", message: json.error ?? `HTTP ${res.status}` });
				} else {
					const text = await res.text().catch(() => "");
					setSubscribeStatus({ type: "error", message: text || `HTTP ${res.status}` });
				}
				return;
			}

			const contentType = res.headers.get("content-type") ?? "";
			if (contentType.includes("application/json")) {
				const json = (await res.json().catch(() => ({}))) as { status?: string };
				if (json.status === "active") {
					setSubscribeStatus({
						type: "success",
						message: "You're already subscribed. Preference updated.",
					});
					return;
				}
			}

			setSubscribeStatus({
				type: "success",
				message: "Check your email to confirm your subscription.",
			});
		} catch (err) {
			setSubscribeStatus({
				type: "error",
				message: err instanceof Error ? err.message : String(err),
			});
		}
	}

	return (
		<DocsLayout>
			<div className="mx-auto max-w-3xl">
				<header className="mb-10">
					<h1 className="text-3xl font-extrabold tracking-tight">Updates</h1>
					<p className="mt-2 text-slate-600 dark:text-slate-300">
						Product updates, release announcements, and engineering notes.
					</p>
				</header>

				<section>
					<div className="flex flex-wrap items-center justify-between gap-3">
						<h2 className="text-xl font-bold">Posts</h2>
						<div className="flex items-center gap-2">
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

							<IconButton
								aria-label="Subscribe"
								title="Subscribe"
								onClick={() => {
									setSubscribeTo(filter === "post" || filter === "release" ? filter : "all");
									setSubscribeOpen(true);
								}}
							>
								<BellNotification className="h-5 w-5" />
							</IconButton>
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

				{subscribeOpen ? (
					<div className="fixed inset-0 z-50">
						<button
							type="button"
							aria-label="Close dialog"
							className="absolute inset-0 bg-slate-950/40 backdrop-blur-sm"
							onClick={() => setSubscribeOpen(false)}
						/>
						<div
							role="dialog"
							aria-modal="true"
							aria-label="Subscribe to updates"
							className="absolute left-1/2 top-24 w-[min(560px,calc(100vw-2rem))] -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-5 shadow-xl dark:border-slate-800 dark:bg-slate-950"
						>
							<div className="flex items-center justify-between gap-3">
								<div className="text-base font-bold">Subscribe</div>
								<IconButton
									aria-label="Close"
									title="Close"
									onClick={() => setSubscribeOpen(false)}
								>
									<Xmark className="h-5 w-5" />
								</IconButton>
							</div>

							<div className="mt-4 grid gap-3">
								<div className="grid gap-2">
									<div className="flex items-center justify-between gap-3">
										<div className="text-sm font-semibold">Feed</div>
										<div className="flex gap-2 text-sm">
											<a
												className="rounded-full bg-slate-100 px-3 py-1 font-semibold hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800"
												href={feedRssUrl}
												target="_blank"
												rel="noreferrer"
											>
												RSS
											</a>
											<a
												className="rounded-full bg-slate-100 px-3 py-1 font-semibold hover:bg-slate-200 dark:bg-slate-900 dark:hover:bg-slate-800"
												href={feedJsonUrl}
												target="_blank"
												rel="noreferrer"
											>
												JSON
											</a>
										</div>
									</div>

									<div className="grid gap-2 text-sm">
										<label className="flex items-center gap-2">
											<input
												type="checkbox"
												checked={subscribeTo === "all"}
												onChange={() => setSubscribeTo("all")}
											/>
											<span className="font-semibold">All</span>
										</label>
										<label className="flex items-center gap-2">
											<input
												type="checkbox"
												checked={subscribeTo === "post"}
												onChange={() => setSubscribeTo("post")}
											/>
											<span className="font-semibold">Posts</span>
										</label>
										<label className="flex items-center gap-2">
											<input
												type="checkbox"
												checked={subscribeTo === "release"}
												onChange={() => setSubscribeTo("release")}
											/>
											<span className="font-semibold">Releases</span>
										</label>
									</div>
								</div>

								<form className="flex w-full gap-2" onSubmit={onSubscribe}>
									<input
										className="w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-slate-300 dark:border-slate-800 dark:bg-slate-950 dark:focus:ring-slate-700"
										type="email"
										placeholder="you@example.com"
										required
										disabled={!emailFormAction}
										value={email}
										onChange={(e) => setEmail(e.target.value)}
									/>
									<button
										className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:cursor-not-allowed disabled:opacity-50 dark:bg-slate-50 dark:text-slate-950"
										type="submit"
										disabled={!emailFormAction || subscribeStatus.type === "loading"}
										title={
											emailFormAction
												? "Subscribe"
												: "Set VITE_UPDATES_EMAIL_FORM_ACTION to enable email signup"
										}
									>
										{subscribeStatus.type === "loading" ? "Subscribing…" : "Subscribe"}
									</button>
								</form>

								{subscribeStatus.type === "success" ? (
									<div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-900 dark:border-emerald-900/50 dark:bg-emerald-950/40 dark:text-emerald-100">
										{subscribeStatus.message}
									</div>
								) : null}

								{subscribeStatus.type === "error" ? (
									<div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-900 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-200">
										{subscribeStatus.message}
									</div>
								) : null}
							</div>
						</div>
					</div>
				) : null}
			</div>
		</DocsLayout>
	);
}
