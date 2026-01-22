export type Theme = "light" | "dark";

export const THEME_STORAGE_KEY = "peerbit-theme";

export function getThemeFromStorage(): Theme | null {
	if (typeof window === "undefined") return null;
	const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
	return stored === "light" || stored === "dark" ? stored : null;
}

export function getSystemTheme(): Theme {
	if (typeof window === "undefined") return "light";
	return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
		? "dark"
		: "light";
}

export function getActiveTheme(): Theme {
	if (typeof document === "undefined") return "light";
	return document.documentElement.classList.contains("dark") ? "dark" : "light";
}

export function setTheme(theme: Theme) {
	if (typeof document === "undefined") return;
	document.documentElement.classList.toggle("dark", theme === "dark");
	try {
		window.localStorage.setItem(THEME_STORAGE_KEY, theme);
	} catch {
		// ignore
	}
}

