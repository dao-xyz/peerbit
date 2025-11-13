export const BASE_URL = process.env.BASE_URL || "http://localhost:5190";

export const OFFLINE_BASE = (() => {
    const base = BASE_URL.replace(/\/+$/, "");
    if (/[?&]bootstrap=/.test(base)) {
        return base.includes("#") ? base : base + "#/";
    }
    const sep = base.includes("?") ? "&" : "?";
    return `${base}${sep}bootstrap=offline#/`;
})();

/**
 * Append search params to the URL before any hash fragment.
 * Ensures parameters end up in window.location.search (not inside the hash),
 * which our app parses and merges with hash query params.
 */
export function withSearchParams(
    url: string,
    params: Record<string, string | number | boolean | undefined>
) {
    const [head, hash = ""] = url.split("#");
    const u = new URL(head);
    for (const [k, v] of Object.entries(params)) {
        if (v === undefined) continue;
        u.searchParams.set(k, String(v));
    }
    return u.toString() + (hash ? `#${hash}` : "");
}
