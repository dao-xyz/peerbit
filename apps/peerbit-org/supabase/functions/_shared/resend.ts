type SendEmailArgs = {
	to: string;
	from: string;
	subject: string;
	html: string;
	text: string;
};

export async function sendResendEmail(args: SendEmailArgs) {
	const apiKey = Deno.env.get("RESEND_API_KEY");
	if (!apiKey) throw new Error("Missing RESEND_API_KEY");

	const res = await fetch("https://api.resend.com/emails", {
		method: "POST",
		headers: {
			"Authorization": `Bearer ${apiKey}`,
			"Content-Type": "application/json",
		},
		body: JSON.stringify({
			from: args.from,
			to: [args.to],
			subject: args.subject,
			html: args.html,
			text: args.text,
		}),
	});

	if (!res.ok) {
		const body = await res.text().catch(() => "");
		throw new Error(`Resend error: HTTP ${res.status} ${body}`);
	}

	return await res.json().catch(() => ({}));
}

