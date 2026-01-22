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
		<div className="flex h-full min-h-[148px] flex-col rounded-xl border border-slate-200 bg-white p-5 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md dark:border-slate-800 dark:bg-slate-950">
			<div className="text-base font-semibold">{title}</div>
			{description ? (
				<div className="mt-1 text-sm leading-6 text-slate-600 dark:text-slate-300">
					{description}
				</div>
			) : null}
		</div>
	);

	return href ? (
		<a href={href} className="block h-full">
			{inner}
		</a>
	) : (
		inner
	);
}
