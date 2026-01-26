import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

import { corsHeaders } from "../_shared/cors.ts";

function redirectTo(url: string, headers: Record<string, string>, status = 303) {
	return new Response(null, {
		status,
		headers: {
			...headers,
			"Cache-Control": "no-store",
			Location: url,
		},
	});
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

	const base = siteUrl.replace(/\/$/, "");
	const successUrl = `${base}/#/updates?unsubscribed=1`;
	const errorUrl = `${base}/#/updates?unsubscribed=0`;

	if (!email || !token) {
		return redirectTo(`${errorUrl}&reason=missing`, headers);
	}

	const supabase = createClient(supabaseUrl, serviceKey, { auth: { persistSession: false } });

	const { data: row } = await supabase
		.from("updates_subscribers")
		.select("email,unsubscribe_token,status")
		.eq("email", email)
		.maybeSingle<{ email: string; unsubscribe_token: string; status: string }>();

	if (!row || row.unsubscribe_token !== token) {
		return redirectTo(`${errorUrl}&reason=invalid`, headers);
	}

	const { error: updateError } = await supabase
		.from("updates_subscribers")
		.update({
			status: "unsubscribed",
			unsubscribed_at: new Date().toISOString(),
		})
		.eq("email", email);

	if (updateError) {
		return redirectTo(`${errorUrl}&reason=error`, headers);
	}

	return redirectTo(successUrl, headers);
});
