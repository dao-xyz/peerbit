export type Message =
	| { type: "init"; storage: "in-memory" | "disc" }
	| { type: "insert"; docs: number; size?: number }
	| { type: "done" }
	| { type: "ready" };
