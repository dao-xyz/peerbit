import type { ReactNode } from "react";

export function Container({
	className = "",
	children,
}: {
	className?: string;
	children: ReactNode;
}) {
	return <div className={["mx-auto w-full max-w-6xl px-4", className].join(" ")}>{children}</div>;
}
