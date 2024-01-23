import React from "react";
import ReactDOM from "react-dom/client";
import { createStore } from "@peerbit/any-store";

(window as any)["create"] = async (
	type: "disc" | "memory",
	dir?: string | undefined
) => {
	const store = createStore(
		type === "disc" ? dir || "./tmp/" + (+new Date()).toString() : undefined
	);
	await store.open();
	(window as any)["store"] = store;
};

const root = ReactDOM.createRoot(
	document.getElementById("root") as HTMLElement
);
root.render(
	<React.StrictMode>
		<>Hello</>
	</React.StrictMode>
);
