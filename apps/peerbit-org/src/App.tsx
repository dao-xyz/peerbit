import { HashRouter, Navigate, Route, Routes } from "react-router-dom";

import { AppLayout } from "./layout/AppLayout";
import { HomePage } from "./pages/HomePage";
import { MarkdownDocPage } from "./pages/MarkdownDocPage";
import { StatusPage } from "./pages/StatusPage";
import { UpdatesPage } from "./pages/UpdatesPage";

export default function App() {
	return (
		<HashRouter>
			<Routes>
				<Route element={<AppLayout />}>
					<Route index element={<HomePage />} />
					<Route path="status" element={<StatusPage />} />

					<Route path="docs" element={<Navigate to="/docs/getting-started" replace />} />
					<Route path="docs/*" element={<MarkdownDocPage base="content/docs" />} />

					<Route path="updates" element={<UpdatesPage />} />
					<Route path="blog" element={<Navigate to="/updates" replace />} />
					<Route
						path="release-notes"
						element={<MarkdownDocPage base="content/docs" doc="release-notes.md" />}
					/>

					<Route path="*" element={<MarkdownDocPage base="content/docs" />} />
				</Route>
			</Routes>
		</HashRouter>
	);
}
