import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

import { corsHeaders } from "../_shared/cors.ts";
import { sha256Hex } from "../_shared/crypto.ts";

function htmlRedirect(title: string, message: string, redirectTo: string) {
	const safeTitle = title.replace(/</g, "&lt;").replace(/>/g, "&gt;");
	const safeMessage = message.replace(/</g, "&lt;").replace(/>/g, "&gt;");
	return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta http-equiv="refresh" content="2;url=${redirectTo}" />
    <title>${safeTitle}</title>
  </head>
  <body style="font-family: ui-sans-serif, system-ui, sans-serif; padding: 24px; line-height: 1.5;">
    <h1 style="margin: 0 0 8px 0; font-size: 18px;">${safeTitle}</h1>
    <p style="margin: 0 0 16px 0; color: #334155;">${safeMessage}</p>
    <p style="margin: 0;"><a href="${redirectTo}">Continue</a></p>
  </body>
</html>`;
}

Deno.serve(async (req) => {
	const headers = corsHeaders(req);
	if (req.method === "OPTIONS") return new Response("ok", { headers });
	if (req.method !== "GET") return new Response("Method not allowed", { status: 405, headers });

	const supabaseUrl = Deno.env.get("SUPABASE_URL");
	const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
	const siteUrl = Deno.env.get("SITE_URL") ?? "https://peerbit.org";
	if (!supabaseUrl || !serviceKey) return new Response("Missing Supabase config", { status: 500, headers });

	const url = new URL(req.url);
	const email = (url.searchParams.get("email") ?? "").trim().toLowerCase();
	const token = url.searchParams.get("token") ?? "";

	const redirectTo = `${siteUrl}/#/updates?confirmed=1`;

	if (!email || !token) {
		const body = htmlRedirect("Invalid link", "Missing email or token.", redirectTo);
		return new Response(body, { status: 400, headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
	}

	const tokenHash = await sha256Hex(token);
	const supabase = createClient(supabaseUrl, serviceKey, { auth: { persistSession: false } });

	const { data: row, error: rowError } = await supabase
		.from("updates_subscribers")
		.select("email,confirm_token_expires_at,status")
		.eq("email", email)
		.eq("confirm_token_hash", tokenHash)
		.maybeSingle<{ email: string; confirm_token_expires_at: string | null; status: string }>();

	if (rowError) {
		const body = htmlRedirect("Error", "Failed to confirm subscription.", redirectTo);
		return new Response(body, { status: 500, headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
	}

	if (!row || row.status !== "pending") {
		const body = htmlRedirect("Invalid link", "This confirmation link is invalid or already used.", redirectTo);
		return new Response(body, { status: 400, headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
	}

	if (row.confirm_token_expires_at) {
		const expires = new Date(row.confirm_token_expires_at);
		if (!Number.isNaN(expires.getTime()) && expires.getTime() < Date.now()) {
			const body = htmlRedirect("Expired link", "This confirmation link has expired. Please subscribe again.", redirectTo);
			return new Response(body, { status: 400, headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
		}
	}

	await supabase
		.from("updates_subscribers")
		.update({
			status: "active",
			confirm_token_hash: null,
			confirm_token_expires_at: null,
			confirm_sent_at: null,
			confirmed_at: new Date().toISOString(),
		})
		.eq("email", email);

	const body = htmlRedirect("Subscribed", "You're subscribed to Peerbit Updates.", redirectTo);
	return new Response(body, { headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
});

