import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

import { corsHeaders } from "../_shared/cors.ts";
import { isValidEmail, randomToken, sha256Hex } from "../_shared/crypto.ts";
import { sendResendEmail } from "../_shared/resend.ts";

type Topic = "all" | "post" | "release";

type SubscriberRow = {
	email: string;
	status: "pending" | "active" | "unsubscribed";
	topic: Topic;
	confirm_sent_at: string | null;
	unsubscribe_token: string;
};

function parseTopic(value: unknown): Topic {
	if (value === "post" || value === "release" || value === "all") return value;
	return "all";
}

function minutesBetween(a: Date, b: Date) {
	return Math.abs(a.getTime() - b.getTime()) / 1000 / 60;
}

function htmlPage(title: string, message: string, siteUrl: string) {
	const safeTitle = title.replace(/</g, "&lt;").replace(/>/g, "&gt;");
	const safeMessage = message.replace(/</g, "&lt;").replace(/>/g, "&gt;");
	return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${safeTitle}</title>
  </head>
  <body style="font-family: ui-sans-serif, system-ui, sans-serif; padding: 24px; line-height: 1.5;">
    <h1 style="margin: 0 0 8px 0; font-size: 18px;">${safeTitle}</h1>
    <p style="margin: 0 0 16px 0; color: #334155;">${safeMessage}</p>
    <p style="margin: 0;">
      <a href="${siteUrl}/#/updates">Back to Updates</a>
    </p>
  </body>
</html>`;
}

async function readBody(req: Request) {
	const contentType = req.headers.get("content-type") ?? "";
	if (contentType.includes("application/json")) {
		const body = (await req.json().catch(() => ({}))) as Record<string, unknown>;
		return { email: body.email, topic: body.topic };
	}

	const form = await req.formData();
	return { email: form.get("email"), topic: form.get("topic") };
}

Deno.serve(async (req) => {
	const headers = corsHeaders(req);
	if (req.method === "OPTIONS") return new Response("ok", { headers });
	if (req.method !== "POST") return new Response("Method not allowed", { status: 405, headers });

	const contentType = req.headers.get("content-type") ?? "";
	const accept = req.headers.get("accept") ?? "";
	const wantsJson = contentType.includes("application/json") || accept.includes("application/json");

	const supabaseUrl = Deno.env.get("SUPABASE_URL");
	const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
	const resendFrom = Deno.env.get("RESEND_FROM");
	const siteUrl = Deno.env.get("SITE_URL") ?? "https://peerbit.org";

	if (!supabaseUrl || !serviceKey) {
		return new Response(
			wantsJson ? JSON.stringify({ error: "Missing Supabase config" }) : "Missing Supabase config",
			{ status: 500, headers: wantsJson ? { ...headers, "Content-Type": "application/json; charset=utf-8" } : headers },
		);
	}
	if (!resendFrom) {
		return new Response(
			wantsJson ? JSON.stringify({ error: "Missing RESEND_FROM" }) : "Missing RESEND_FROM",
			{ status: 500, headers: wantsJson ? { ...headers, "Content-Type": "application/json; charset=utf-8" } : headers },
		);
	}

	const { email: rawEmail, topic: rawTopic } = await readBody(req);
	const email = typeof rawEmail === "string" ? rawEmail.trim().toLowerCase() : "";
	const topic = parseTopic(typeof rawTopic === "string" ? rawTopic : String(rawTopic ?? ""));

	if (!email || !isValidEmail(email)) {
		if (wantsJson) {
			return new Response(JSON.stringify({ error: "Invalid email" }), {
				status: 400,
				headers: { ...headers, "Content-Type": "application/json; charset=utf-8" },
			});
		}
		const body = htmlPage("Subscribe", "Please enter a valid email address.", siteUrl);
		return new Response(body, {
			status: 400,
			headers: { ...headers, "Content-Type": "text/html; charset=utf-8" },
		});
	}

	const supabase = createClient(supabaseUrl, serviceKey, {
		auth: { persistSession: false },
	});

	const { data: existing, error: existingError } = await supabase
		.from("updates_subscribers")
		.select("email,status,topic,confirm_sent_at,unsubscribe_token")
		.eq("email", email)
		.maybeSingle<SubscriberRow>();

	if (existingError) {
		return new Response(
			wantsJson
				? JSON.stringify({ error: `DB error: ${existingError.message}` })
				: `DB error: ${existingError.message}`,
			{ status: 500, headers: wantsJson ? { ...headers, "Content-Type": "application/json; charset=utf-8" } : headers },
		);
	}

	if (existing?.status === "active") {
		if (existing.topic !== topic) {
			await supabase
				.from("updates_subscribers")
				.update({ topic })
				.eq("email", email);
		}
		if (wantsJson) {
			return new Response(JSON.stringify({ ok: true, status: "active", topic }), {
				headers: { ...headers, "Content-Type": "application/json; charset=utf-8" },
			});
		}
		const body = htmlPage("Subscribed", "You're already subscribed. Preference updated.", siteUrl);
		return new Response(body, { headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
	}

	const throttleMinutes = Number(Deno.env.get("UPDATES_CONFIRM_THROTTLE_MINUTES") ?? "5");
	if (existing?.confirm_sent_at) {
		const last = new Date(existing.confirm_sent_at);
		if (!Number.isNaN(last.getTime()) && minutesBetween(new Date(), last) < throttleMinutes) {
			if (wantsJson) {
				return new Response(JSON.stringify({ ok: true, status: "pending" }), {
					headers: { ...headers, "Content-Type": "application/json; charset=utf-8" },
				});
			}
			const body = htmlPage(
				"Check your email",
				"A confirmation email was recently sent. Please check your inbox.",
				siteUrl,
			);
			return new Response(body, { headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
		}
	}

	const confirmToken = randomToken();
	const confirmHash = await sha256Hex(confirmToken);
	const ttlMinutes = Number(Deno.env.get("UPDATES_CONFIRM_TTL_MINUTES") ?? "1440");
	const expiresAt = new Date(Date.now() + ttlMinutes * 60 * 1000).toISOString();
	const unsubscribeToken = existing?.unsubscribe_token ?? randomToken();

	const { error: upsertError } = await supabase
		.from("updates_subscribers")
		.upsert(
			{
				email,
				topic,
				status: "pending",
				confirm_token_hash: confirmHash,
				confirm_token_expires_at: expiresAt,
				confirm_sent_at: new Date().toISOString(),
				unsubscribe_token: unsubscribeToken,
				unsubscribed_at: null,
			},
			{ onConflict: "email" },
		);

	if (upsertError) {
		return new Response(
			wantsJson
				? JSON.stringify({ error: `DB error: ${upsertError.message}` })
				: `DB error: ${upsertError.message}`,
			{ status: 500, headers: wantsJson ? { ...headers, "Content-Type": "application/json; charset=utf-8" } : headers },
		);
	}

	const functionsBaseUrl = `${supabaseUrl.replace(/\/$/, "")}/functions/v1`;
	const confirmUrl =
		`${functionsBaseUrl}/updates-confirm?email=${encodeURIComponent(email)}&token=${encodeURIComponent(confirmToken)}`;

	const subject = "Confirm your Peerbit Updates subscription";
	const html = `
<div style="font-family: ui-sans-serif, system-ui, sans-serif; line-height: 1.6;">
  <h2 style="margin: 0 0 12px 0;">Confirm subscription</h2>
  <p style="margin: 0 0 12px 0;">Click the button below to confirm your subscription to Peerbit Updates.</p>
  <p style="margin: 0 0 18px 0;">
    <a href="${confirmUrl}" style="display:inline-block;background:#0f172a;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:700;">Confirm</a>
  </p>
  <p style="margin: 0; color: #64748b; font-size: 12px;">If you didn’t request this, you can ignore this email.</p>
</div>`;
	const text = `Confirm your subscription: ${confirmUrl}\n\nIf you didn’t request this, you can ignore this email.`;

	await sendResendEmail({ to: email, from: resendFrom, subject, html, text });

	if (wantsJson) {
		return new Response(JSON.stringify({ ok: true, status: "pending" }), {
			headers: { ...headers, "Content-Type": "application/json; charset=utf-8" },
		});
	}

	const body = htmlPage("Check your email", "We sent a confirmation email. Please confirm to finish subscribing.", siteUrl);
	return new Response(body, { headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
});
