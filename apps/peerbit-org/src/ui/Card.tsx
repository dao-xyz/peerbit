export function Card({
	title,
	description,
	href,
}: {
	title: string;
	description: string;
	href?: string;
}) {
	const inner = (
		<div className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md dark:border-slate-800 dark:bg-slate-950">
			<div className="text-base font-semibold">{title}</div>
			{description ? (
				<div className="mt-1 text-sm text-slate-600 dark:text-slate-300">
					{description}
				</div>
			) : null}
		</div>
	);

	return href ? (
		<a href={href} className="block">
			{inner}
		</a>
	) : (
		inner
	);
}

