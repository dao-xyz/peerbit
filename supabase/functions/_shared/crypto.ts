function base64UrlEncode(bytes: Uint8Array) {
	return btoa(String.fromCharCode(...bytes))
		.replace(/\+/g, "-")
		.replace(/\//g, "_")
		.replace(/=+$/g, "");
}

export function randomToken(byteLength = 32) {
	const bytes = new Uint8Array(byteLength);
	crypto.getRandomValues(bytes);
	return base64UrlEncode(bytes);
}

export async function sha256Hex(value: string) {
	const data = new TextEncoder().encode(value);
	const digest = await crypto.subtle.digest("SHA-256", data);
	const bytes = new Uint8Array(digest);
	return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

export function isValidEmail(email: string) {
	// Basic sanity check (not RFC exhaustive)
	return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

