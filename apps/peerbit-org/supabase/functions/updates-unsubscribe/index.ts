import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

import { corsHeaders } from "../_shared/cors.ts";

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

	const redirectTo = `${siteUrl}/#/updates?unsubscribed=1`;

	if (!email || !token) {
		const body = htmlRedirect("Invalid link", "Missing email or token.", redirectTo);
		return new Response(body, { status: 400, headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
	}

	const supabase = createClient(supabaseUrl, serviceKey, { auth: { persistSession: false } });

	const { data: row } = await supabase
		.from("updates_subscribers")
		.select("email,unsubscribe_token,status")
		.eq("email", email)
		.maybeSingle<{ email: string; unsubscribe_token: string; status: string }>();

	if (!row || row.unsubscribe_token !== token) {
		const body = htmlRedirect("Invalid link", "This unsubscribe link is invalid.", redirectTo);
		return new Response(body, { status: 400, headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
	}

	await supabase
		.from("updates_subscribers")
		.update({
			status: "unsubscribed",
			unsubscribed_at: new Date().toISOString(),
		})
		.eq("email", email);

	const body = htmlRedirect("Unsubscribed", "You've been unsubscribed from Peerbit Updates.", redirectTo);
	return new Response(body, { headers: { ...headers, "Content-Type": "text/html; charset=utf-8" } });
});

