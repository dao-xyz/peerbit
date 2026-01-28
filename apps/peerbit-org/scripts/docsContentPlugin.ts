import fs from "node:fs";
import path from "node:path";
import type { Plugin, ResolvedConfig } from "vite";

type UpdateKind = "post" | "release";

type UpdatesIndexItem = {
	kind: UpdateKind;
	title: string;
	date: string; // YYYY-MM-DD
	href: string;
	excerpt?: string;
};

function xmlEscape(value: string) {
	return value
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;")
		.replace(/'/g, "&#039;");
}

function stripMarkdown(value: string) {
	return value
		.replace(/!\[[^\]]*]\([^)]*\)/g, "")
		.replace(/\[[^\]]*]\(([^)]*)\)/g, (_m, _href) => "")
		.replace(/`([^`]+)`/g, "$1")
		.replace(/\*\*([^*]+)\*\*/g, "$1")
		.replace(/\*([^*]+)\*/g, "$1")
		.replace(/_([^_]+)_/g, "$1")
		.replace(/#+\s+/g, "")
		.replace(/\s+/g, " ")
		.trim();
}

function extractTitle(markdown: string) {
	const lines = markdown.split(/\r?\n/);
	const titleLine = lines.find((l) => l.startsWith("# "));
	return titleLine ? titleLine.replace(/^#\s+/, "").trim() : "Untitled";
}

function dateFromFilename(filename: string) {
	const match = filename.match(/^(\d{4}-\d{2}-\d{2})-/);
	return match?.[1] ?? null;
}

function extractExcerpt(markdown: string) {
	const lines = markdown.split(/\r?\n/);
	let i = 0;

	while (i < lines.length && !lines[i].startsWith("# ")) i++;
	if (i < lines.length) i++;
	while (i < lines.length && lines[i].trim() === "") i++;

	const maybeDate = lines[i]?.trim() ?? "";
	if (/^\*.*\*\s*$/.test(maybeDate) && /\d{4}/.test(maybeDate)) {
		i++;
		while (i < lines.length && lines[i].trim() === "") i++;
	}

	const paragraph: string[] = [];
	while (i < lines.length && lines[i].trim() !== "") {
		paragraph.push(lines[i].trim());
		i++;
	}

	return paragraph.length ? stripMarkdown(paragraph.join(" ")) : undefined;
}

function docPathToHref(docPath: string) {
	const withoutMd = docPath.replace(/\.md$/, "");
	if (withoutMd.endsWith("/README")) return `/docs/${withoutMd.replace(/\/README$/, "")}`;
	return `/docs/${withoutMd}`;
}

function listMarkdownFilesRecursive(dir: string) {
	if (!fs.existsSync(dir)) return [];
	const out: string[] = [];
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		if (entry.name.startsWith(".")) continue;
		const abs = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			out.push(...listMarkdownFilesRecursive(abs));
		} else if (entry.isFile() && entry.name.endsWith(".md")) {
			out.push(abs);
		}
	}
	return out;
}

function collectUpdates(docsDir: string): UpdatesIndexItem[] {
	const items: UpdatesIndexItem[] = [];

	const postsDir = path.join(docsDir, "blog");
	for (const abs of listMarkdownFilesRecursive(postsDir)) {
		const rel = path.relative(docsDir, abs).replace(/\\/g, "/");
		const basename = path.basename(abs);
		const date = dateFromFilename(basename) ?? "1970-01-01";
		const markdown = fs.readFileSync(abs, "utf8");
		items.push({
			kind: "post",
			title: extractTitle(markdown),
			date,
			href: docPathToHref(rel),
			excerpt: extractExcerpt(markdown),
		});
	}

	// Future: add releases from docs/releases/* or similar.
	return items.sort((a, b) => b.date.localeCompare(a.date));
}

function buildRssFeed({
	siteUrl,
	title,
	feedUrl,
	items,
}: {
	siteUrl: string;
	title: string;
	feedUrl: string;
	items: UpdatesIndexItem[];
}) {
	const updated = new Date().toUTCString();
	const channelLink = `${siteUrl}/#/updates`;

	const itemXml = items
		.map((i) => {
			const link = `${siteUrl}/#${i.href}`;
			const pubDate = new Date(`${i.date}T00:00:00Z`).toUTCString();
			const desc = i.excerpt ? `<![CDATA[${i.excerpt}]]>` : "";
			return [
				"<item>",
				`<title>${xmlEscape(i.title)}</title>`,
				`<link>${xmlEscape(link)}</link>`,
				`<guid isPermaLink=\"true\">${xmlEscape(link)}</guid>`,
				`<pubDate>${xmlEscape(pubDate)}</pubDate>`,
				desc ? `<description>${desc}</description>` : "",
				"</item>",
			]
				.filter(Boolean)
				.join("");
		})
		.join("");

	return [
		'<?xml version="1.0" encoding="UTF-8"?>',
		'<rss version="2.0">',
		"<channel>",
		`<title>${xmlEscape(title)}</title>`,
		`<link>${xmlEscape(channelLink)}</link>`,
		`<description>${xmlEscape("Peerbit updates and release announcements")}</description>`,
		`<language>en</language>`,
		`<lastBuildDate>${xmlEscape(updated)}</lastBuildDate>`,
		`<atom:link href="${xmlEscape(feedUrl)}" rel="self" type="application/rss+xml" xmlns:atom="http://www.w3.org/2005/Atom" />`,
		itemXml,
		"</channel>",
		"</rss>",
	].join("\n");
}

function buildJsonFeed({
	siteUrl,
	title,
	feedUrl,
	items,
}: {
	siteUrl: string;
	title: string;
	feedUrl: string;
	items: UpdatesIndexItem[];
}) {
	return JSON.stringify(
		{
			version: "https://jsonfeed.org/version/1.1",
			title,
			home_page_url: `${siteUrl}/#/updates`,
			feed_url: feedUrl,
			items: items.map((i) => {
				const url = `${siteUrl}/#${i.href}`;
				return {
					id: url,
					url,
					title: i.title,
					date_published: new Date(`${i.date}T00:00:00Z`).toISOString(),
					content_text: i.excerpt ?? "",
				};
			}),
		},
		null,
		2,
	);
}

function generateUpdatesArtifacts(docsDir: string, siteUrl: string) {
	const all = collectUpdates(docsDir);
	const posts = all.filter((i) => i.kind === "post");
	const releases = all.filter((i) => i.kind === "release");

	return {
		indexJson: JSON.stringify(all, null, 2),
		rssAll: buildRssFeed({
			siteUrl,
			title: "Peerbit Updates",
			feedUrl: `${siteUrl}/content/docs/updates/all.xml`,
			items: all,
		}),
		rssPosts: buildRssFeed({
			siteUrl,
			title: "Peerbit Updates (Posts)",
			feedUrl: `${siteUrl}/content/docs/updates/posts.xml`,
			items: posts,
		}),
		rssReleases: buildRssFeed({
			siteUrl,
			title: "Peerbit Updates (Releases)",
			feedUrl: `${siteUrl}/content/docs/updates/releases.xml`,
			items: releases,
		}),
		jsonAll: buildJsonFeed({
			siteUrl,
			title: "Peerbit Updates",
			feedUrl: `${siteUrl}/content/docs/updates/all.json`,
			items: all,
		}),
		jsonPosts: buildJsonFeed({
			siteUrl,
			title: "Peerbit Updates (Posts)",
			feedUrl: `${siteUrl}/content/docs/updates/posts.json`,
			items: posts,
		}),
		jsonReleases: buildJsonFeed({
			siteUrl,
			title: "Peerbit Updates (Releases)",
			feedUrl: `${siteUrl}/content/docs/updates/releases.json`,
			items: releases,
		}),
	};
}

export function docsContentPlugin(): Plugin {
	const docsDir = path.resolve(__dirname, "../../../docs");
	const contentPrefix = "/content/docs/";
	const siteUrl = process.env.SITE_URL ?? process.env.VITE_SITE_URL ?? "https://peerbit.org";

	let resolved: ResolvedConfig;

	return {
		name: "peerbit-docs-content",
		configResolved(config) {
			resolved = config;
		},
		configureServer(server) {
			server.middlewares.use((req, res, next) => {
				const url = req.url?.split("?")[0] ?? "";
				if (!url.startsWith(contentPrefix)) return next();

				const requestPath = decodeURIComponent(url.slice(contentPrefix.length));

				if (requestPath.startsWith("updates/")) {
					const artifacts = generateUpdatesArtifacts(docsDir, siteUrl);
					const contentType =
						requestPath.endsWith(".json")
							? "application/json; charset=utf-8"
							: "application/rss+xml; charset=utf-8";
					res.setHeader("Content-Type", contentType);
					res.setHeader("Cache-Control", "no-store");
					if (requestPath === "updates/index.json") return res.end(artifacts.indexJson);
					if (requestPath === "updates/all.xml") return res.end(artifacts.rssAll);
					if (requestPath === "updates/posts.xml") return res.end(artifacts.rssPosts);
					if (requestPath === "updates/releases.xml") return res.end(artifacts.rssReleases);
					if (requestPath === "updates/all.json") return res.end(artifacts.jsonAll);
					if (requestPath === "updates/posts.json") return res.end(artifacts.jsonPosts);
					if (requestPath === "updates/releases.json") return res.end(artifacts.jsonReleases);
				}

				const absPath = path.resolve(docsDir, requestPath);

				if (!absPath.startsWith(docsDir + path.sep)) {
					res.statusCode = 403;
					res.end("Forbidden");
					return;
				}

				let stat: fs.Stats;
				try {
					stat = fs.statSync(absPath);
				} catch {
					res.statusCode = 404;
					res.end("Not found");
					return;
				}

				if (!stat.isFile()) {
					res.statusCode = 404;
					res.end("Not found");
					return;
				}

				const ext = path.extname(absPath).toLowerCase();
				const contentType =
					ext === ".md"
						? "text/markdown; charset=utf-8"
						: ext === ".ts"
							? "text/plain; charset=utf-8"
							: ext === ".json"
								? "application/json; charset=utf-8"
								: ext === ".xml"
									? "application/rss+xml; charset=utf-8"
									: ext === ".png"
										? "image/png"
										: ext === ".gif"
											? "image/gif"
											: ext === ".svg"
												? "image/svg+xml"
												: ext === ".ico"
													? "image/x-icon"
													: "application/octet-stream";
				res.setHeader("Content-Type", contentType);
				res.setHeader("Cache-Control", "no-store");

				fs.createReadStream(absPath).pipe(res);
			});
		},
		closeBundle() {
			const outDir = path.resolve(resolved.root, resolved.build.outDir);
			const destDir = path.join(outDir, "content", "docs");

			fs.rmSync(destDir, { recursive: true, force: true });
			fs.mkdirSync(destDir, { recursive: true });

			fs.cpSync(docsDir, destDir, {
				recursive: true,
				filter(src) {
					const rel = path.relative(docsDir, src);
					if (rel.startsWith("..")) return false;
					if (rel === "") return true;
					if (rel === "node_modules" || rel.startsWith(`node_modules${path.sep}`)) return false;
					if (rel === "dist" || rel.startsWith(`dist${path.sep}`)) return false;
					return true;
				},
			});

			const artifacts = generateUpdatesArtifacts(docsDir, siteUrl);
			const updatesOutDir = path.join(destDir, "updates");
			fs.mkdirSync(updatesOutDir, { recursive: true });
			fs.writeFileSync(path.join(updatesOutDir, "index.json"), artifacts.indexJson);
			fs.writeFileSync(path.join(updatesOutDir, "all.xml"), artifacts.rssAll);
			fs.writeFileSync(path.join(updatesOutDir, "posts.xml"), artifacts.rssPosts);
			fs.writeFileSync(path.join(updatesOutDir, "releases.xml"), artifacts.rssReleases);
			fs.writeFileSync(path.join(updatesOutDir, "all.json"), artifacts.jsonAll);
			fs.writeFileSync(path.join(updatesOutDir, "posts.json"), artifacts.jsonPosts);
			fs.writeFileSync(path.join(updatesOutDir, "releases.json"), artifacts.jsonReleases);
		},
	};
}
