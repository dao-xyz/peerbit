import { Outlet } from "react-router-dom";

import { Header } from "./Header";

export function AppLayout() {
	return (
		<div className="min-h-screen">
			<Header />
			<div className="mx-auto w-full max-w-6xl px-4 py-8">
				<Outlet />
			</div>
		</div>
	);
}

