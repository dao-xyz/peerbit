import { useEffect, useState } from "react";

import { Card } from "../ui/Card";

type BootstrapStatus = {
	generatedAt: string | null;
	nodes: { address: string; ok: boolean; checkedAt: string; detail?: string }[];
};

export function StatusPage() {
	const [status, setStatus] = useState<BootstrapStatus | null>(null);
	const [error, setError] = useState<string | null>(null);

	useEffect(() => {
		(async () => {
			try {
				const res = await fetch("content/docs/status/bootstrap.json", {
					cache: "no-store",
				});
				if (!res.ok) throw new Error(`HTTP ${res.status}`);
				setStatus((await res.json()) as BootstrapStatus);
			} catch (e) {
				setError(e instanceof Error ? e.message : String(e));
			}
		})();
	}, []);

	return (
		<div className="mx-auto max-w-3xl">
			<h1 className="text-3xl font-bold tracking-tight">Network status</h1>
			<p className="mt-2 text-slate-600 dark:text-slate-300">
				Bootstrap node health and future network indicators.
			</p>

			<div className="mt-6">
				<Card title="Bootstrap health" description="" />
				<div className="mt-3 rounded-xl border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-950">
					{error ? (
						<p className="text-sm text-red-600 dark:text-red-400">{error}</p>
					) : !status ? (
						<p className="text-sm text-slate-500">Loading…</p>
					) : status.nodes.length === 0 ? (
						<p className="text-sm text-slate-500">
							No data yet. (Bootstrap checks not configured.)
						</p>
					) : (
						<div className="overflow-x-auto">
							<table className="w-full text-left text-sm">
								<thead className="text-slate-500">
									<tr>
										<th className="py-2 pr-4">Address</th>
										<th className="py-2 pr-4">Status</th>
										<th className="py-2 pr-4">Checked</th>
										<th className="py-2 pr-4">Detail</th>
									</tr>
								</thead>
								<tbody>
									{status.nodes.map((n) => (
										<tr key={n.address} className="border-t border-slate-200 dark:border-slate-800">
											<td className="py-2 pr-4">
												<code className="break-all">{n.address}</code>
											</td>
											<td className="py-2 pr-4">
												<span
													className={[
														"rounded-full px-2 py-0.5 text-xs font-semibold",
														n.ok
															? "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-200"
															: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-200",
													].join(" ")}
												>
													{n.ok ? "OK" : "DOWN"}
												</span>
											</td>
											<td className="py-2 pr-4">
												<code>{n.checkedAt}</code>
											</td>
											<td className="py-2 pr-4">{n.detail ?? ""}</td>
										</tr>
									))}
								</tbody>
							</table>
						</div>
					)}
				</div>
			</div>
		</div>
	);
}
