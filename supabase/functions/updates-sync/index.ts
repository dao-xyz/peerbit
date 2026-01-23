import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

import { corsHeaders } from "../_shared/cors.ts";
import { sendResendEmail } from "../_shared/resend.ts";

type UpdateKind = "post" | "release";

type UpdatesIndexItem = {
	kind: UpdateKind;
	title: string;
	date: string; // YYYY-MM-DD
	href: string;
	excerpt?: string;
};

type Subscriber = { email: string; topic: "all" | "post" | "release"; unsubscribe_token: string };

function isUpdatesItem(value: unknown): value is UpdatesIndexItem {
	if (!value || typeof value !== "object") return false;
	const v = value as Record<string, unknown>;
	return (
		(v.kind === "post" || v.kind === "release") &&
		typeof v.title === "string" &&
		typeof v.date === "string" &&
		typeof v.href === "string"
	);
}

function requireAuth(req: Request) {
	const secret = Deno.env.get("UPDATES_SYNC_SECRET");
	if (!secret) return { ok: false, status: 500, message: "Missing UPDATES_SYNC_SECRET" } as const;
	const header = req.headers.get("authorization") ?? "";
	const token = header.startsWith("Bearer ") ? header.slice("Bearer ".length) : "";
	if (!token || token !== secret) return { ok: false, status: 401, message: "Unauthorized" } as const;
	return { ok: true } as const;
}

function escapeHtml(value: string) {
	return value
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;")
		.replace(/'/g, "&#039;");
}

function emailBody({
	siteUrl,
	functionsBaseUrl,
	update,
	subscriber,
}: {
	siteUrl: string;
	functionsBaseUrl: string;
	update: UpdatesIndexItem;
	subscriber: Subscriber;
}) {
	const url = `${siteUrl}/#${update.href}`;
	const unsubscribeUrl =
		`${functionsBaseUrl}/updates-unsubscribe?email=${encodeURIComponent(subscriber.email)}&token=${encodeURIComponent(subscriber.unsubscribe_token)}`;
	const title = escapeHtml(update.title);
	const excerpt = update.excerpt ? escapeHtml(update.excerpt) : "";

	const subjectPrefix = update.kind === "release" ? "[Peerbit Release] " : "[Peerbit] ";
	const subject = `${subjectPrefix}${update.title}`;

	const html = `
<div style="font-family: ui-sans-serif, system-ui, sans-serif; line-height: 1.6;">
  <h2 style="margin: 0 0 12px 0;">${title}</h2>
  ${excerpt ? `<p style="margin: 0 0 12px 0; color: #334155;">${excerpt}</p>` : ""}
  <p style="margin: 0 0 18px 0;">
    <a href="${url}" style="display:inline-block;background:#0f172a;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:700;">Read</a>
  </p>
  <hr style="border:0;border-top:1px solid #e2e8f0;margin:18px 0;" />
  <p style="margin: 0; color: #64748b; font-size: 12px;">
    You’re receiving this because you subscribed to Peerbit Updates.
    <a href="${unsubscribeUrl}">Unsubscribe</a>
  </p>
</div>`;

	const text = `${update.title}\n\n${update.excerpt ?? ""}\n\nRead: ${url}\nUnsubscribe: ${unsubscribeUrl}\n`;

	return { subject, html, text };
}

Deno.serve(async (req) => {
	const headers = corsHeaders(req);
	if (req.method === "OPTIONS") return new Response("ok", { headers });
	if (req.method !== "POST") return new Response("Method not allowed", { status: 405, headers });

	const auth = requireAuth(req);
	if (!auth.ok) return new Response(auth.message, { status: auth.status, headers });

	const supabaseUrl = Deno.env.get("SUPABASE_URL");
	const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
	const resendFrom = Deno.env.get("RESEND_FROM");
	const siteUrl = Deno.env.get("SITE_URL") ?? "https://peerbit.org";
	if (!supabaseUrl || !serviceKey) return new Response("Missing Supabase config", { status: 500, headers });
	if (!resendFrom) return new Response("Missing RESEND_FROM", { status: 500, headers });

	const body = await req.json().catch(() => null);
	const items = Array.isArray(body) ? body.filter(isUpdatesItem) : [];

	const supabase = createClient(supabaseUrl, serviceKey, { auth: { persistSession: false } });

	const hrefs = items.map((i) => i.href);
	const { data: alreadySent } = hrefs.length
		? await supabase.from("updates_sent").select("href").in("href", hrefs)
		: { data: [] as { href: string }[] };
	const sentSet = new Set((alreadySent ?? []).map((r) => r.href));
	const toSend = items
		.filter((i) => !sentSet.has(i.href))
		.sort((a, b) => a.date.localeCompare(b.date));

	const functionsBaseUrl = `${supabaseUrl.replace(/\/$/, "")}/functions/v1`;

	const results: Array<{ href: string; sent: number; errors: number }> = [];

	for (const update of toSend) {
		await supabase
			.from("updates_sent")
			.insert({
				kind: update.kind,
				href: update.href,
				title: update.title,
				date: update.date,
				excerpt: update.excerpt ?? null,
			})
			.select()
			.single()
			.catch(() => null);

		const topics = update.kind === "release" ? (["all", "release"] as const) : (["all", "post"] as const);
		const { data: subscribers } = await supabase
			.from("updates_subscribers")
			.select("email,topic,unsubscribe_token")
			.eq("status", "active")
			.in("topic", topics);

		let sent = 0;
		let errors = 0;

		for (const sub of (subscribers ?? []) as Subscriber[]) {
			try {
				const { subject, html, text } = emailBody({ siteUrl, functionsBaseUrl, update, subscriber: sub });
				await sendResendEmail({ to: sub.email, from: resendFrom, subject, html, text });
				sent++;
			} catch {
				errors++;
			}
		}

		results.push({ href: update.href, sent, errors });
	}

	return new Response(JSON.stringify({ processed: toSend.length, results }, null, 2), {
		headers: { ...headers, "Content-Type": "application/json; charset=utf-8" },
	});
});

