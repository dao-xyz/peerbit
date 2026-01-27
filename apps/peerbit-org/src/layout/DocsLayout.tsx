import type { ReactNode } from "react";

import { DocsSidebar } from "./DocsSidebar";

export function DocsLayout({ children }: { children: ReactNode }) {
	return (
		<div className="grid grid-cols-1 gap-8 lg:grid-cols-[280px_minmax(0,1fr)]">
			<aside className="hidden lg:block">
				<div className="sticky top-20 max-h-[calc(100vh-6rem)] overflow-y-auto pr-2">
					<DocsSidebar />
				</div>
			</aside>
			<main>{children}</main>
		</div>
	);
}

