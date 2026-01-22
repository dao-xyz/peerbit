import { useMemo } from "react";
import { useLocation } from "react-router-dom";

import { DocsLayout } from "../layout/DocsLayout";
import { Markdown } from "../ui/Markdown";
import { resolveDocPathFromLocation } from "../utils/resolveDocPath";

export function MarkdownDocPage({
	base,
	doc,
}: {
	base: string;
	doc?: string;
}) {
	const location = useLocation();

	const resolved = useMemo(() => {
		if (doc) return doc;
		return resolveDocPathFromLocation(location.pathname);
	}, [doc, location.pathname]);

	return (
		<DocsLayout>
			<Markdown base={base} docPath={resolved} />
		</DocsLayout>
	);
}
