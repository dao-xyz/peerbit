import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

import { corsHeaders } from "../_shared/cors.ts";
import { sha256Hex } from "../_shared/crypto.ts";

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
	const successUrl = `${base}/#/updates?confirmed=1`;
	const errorUrl = `${base}/#/updates?confirmed=0`;

	if (!email || !token) {
		return redirectTo(`${errorUrl}&reason=missing`, headers);
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
		return redirectTo(`${errorUrl}&reason=error`, headers);
	}

	if (!row || row.status !== "pending") {
		return redirectTo(`${errorUrl}&reason=invalid`, headers);
	}

	if (row.confirm_token_expires_at) {
		const expires = new Date(row.confirm_token_expires_at);
		if (!Number.isNaN(expires.getTime()) && expires.getTime() < Date.now()) {
			return redirectTo(`${errorUrl}&reason=expired`, headers);
		}
	}

	const { error: updateError } = await supabase
		.from("updates_subscribers")
		.update({
			status: "active",
			confirm_token_hash: null,
			confirm_token_expires_at: null,
			confirm_sent_at: null,
			confirmed_at: new Date().toISOString(),
		})
		.eq("email", email);

	if (updateError) {
		return redirectTo(`${errorUrl}&reason=error`, headers);
	}

	return redirectTo(successUrl, headers);
});
