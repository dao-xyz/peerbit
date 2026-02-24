import type { ReactNode } from "react";

export const clamp = (v: number, min: number, max: number) =>
	Math.max(min, Math.min(max, v));

export const readIntAttr = (
	value: unknown,
	fallback: number,
	min: number,
	max: number,
) => {
	const v =
		typeof value === "string"
			? Number(value)
			: typeof value === "number"
				? value
				: NaN;
	if (!Number.isFinite(v)) return fallback;
	return clamp(Math.floor(v), min, max);
};

export const mulberry32 = (seed: number) => {
	let t = seed >>> 0;
	return () => {
		t += 0x6d2b79f5;
		let x = t;
		x = Math.imul(x ^ (x >>> 15), x | 1);
		x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
		return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
	};
};

export const delayMs = (ms: number) =>
	new Promise<void>((resolve) => setTimeout(resolve, ms));

export const InfoPopover = ({ children }: { children: ReactNode }) => (
	<details className="relative inline-block align-middle">
		<summary
			className="inline-flex h-5 w-5 cursor-pointer list-none items-center justify-center rounded-full border border-slate-200 bg-white text-[10px] font-semibold text-slate-600 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800 [&::-webkit-details-marker]:hidden"
			aria-label="Info"
		>
			i
		</summary>
		<div className="absolute right-0 z-20 mt-2 w-72 rounded-xl border border-slate-200 bg-white p-3 text-xs text-slate-700 shadow-lg dark:border-slate-800 dark:bg-slate-950 dark:text-slate-200">
			{children}
		</div>
	</details>
);
