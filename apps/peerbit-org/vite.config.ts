import fs from "node:fs";
import path from "node:path";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import type { Plugin, ResolvedConfig } from "vite";
import { defineConfig } from "vite";

function docsContentPlugin(): Plugin {
	const docsDir = path.resolve(__dirname, "../../docs");
	const contentPrefix = "/content/docs/";

	let resolved: ResolvedConfig;

	return {
		name: "peerbit-docs-content",
		configResolved(config) {
			resolved = config;
		},
		configureServer(server) {
			server.middlewares.use((req, res, next) => {
				const url = req.url?.split("?")[0] ?? "";
				if (!url.startsWith(contentPrefix)) return next();

				const requestPath = decodeURIComponent(url.slice(contentPrefix.length));
				const absPath = path.resolve(docsDir, requestPath);

				if (!absPath.startsWith(docsDir + path.sep)) {
					res.statusCode = 403;
					res.end("Forbidden");
					return;
				}

				let stat: fs.Stats;
				try {
					stat = fs.statSync(absPath);
				} catch {
					res.statusCode = 404;
					res.end("Not found");
					return;
				}

				if (!stat.isFile()) {
					res.statusCode = 404;
					res.end("Not found");
					return;
				}

				const ext = path.extname(absPath).toLowerCase();
				const contentType =
					ext === ".md"
						? "text/markdown; charset=utf-8"
						: ext === ".ts"
							? "text/plain; charset=utf-8"
							: ext === ".json"
								? "application/json; charset=utf-8"
								: ext === ".png"
									? "image/png"
									: ext === ".gif"
										? "image/gif"
										: ext === ".svg"
											? "image/svg+xml"
											: ext === ".ico"
												? "image/x-icon"
												: "application/octet-stream";
				res.setHeader("Content-Type", contentType);
				res.setHeader("Cache-Control", "no-store");

				fs.createReadStream(absPath).pipe(res);
			});
		},
		closeBundle() {
			const outDir = path.resolve(resolved.root, resolved.build.outDir);
			const destDir = path.join(outDir, "content", "docs");

			fs.rmSync(destDir, { recursive: true, force: true });
			fs.mkdirSync(destDir, { recursive: true });

			fs.cpSync(docsDir, destDir, {
				recursive: true,
				filter(src) {
					const rel = path.relative(docsDir, src);
					if (rel.startsWith("..")) return false;
					if (rel === "") return true;
					if (rel === "node_modules" || rel.startsWith(`node_modules${path.sep}`)) return false;
					if (rel === "dist" || rel.startsWith(`dist${path.sep}`)) return false;
					return true;
				},
			});
		},
	};
}

export default defineConfig({
	base: process.env.VITE_BASE ?? "/",
	plugins: [
		tailwindcss(),
		react(),
		docsContentPlugin(),
	],
});
