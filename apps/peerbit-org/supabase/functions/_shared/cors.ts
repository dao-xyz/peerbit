const defaultAllowed = [
	"https://peerbit.org",
	// Vite dev/preview (peerbit-org uses 5193/5194 by default, but keep 5173 for compatibility).
	"http://localhost:5193",
	"http://localhost:5194",
	"http://localhost:5173",
];

function allowedOrigins() {
	const raw = Deno.env.get("UPDATES_ALLOWED_ORIGINS");
	if (!raw) return defaultAllowed;
	return raw
		.split(",")
		.map((s) => s.trim())
		.filter(Boolean);
}

export function corsHeaders(req: Request) {
	const origin = req.headers.get("origin") ?? "";
	const allowed = allowedOrigins();
	const allowOrigin = allowed.includes(origin) ? origin : allowed[0] ?? "https://peerbit.org";

	return {
		"Access-Control-Allow-Origin": allowOrigin,
		"Access-Control-Allow-Methods": "GET,POST,OPTIONS",
		"Access-Control-Allow-Headers": "content-type,authorization",
		"Access-Control-Allow-Credentials": "false",
		"Vary": "Origin",
	};
}
