import { promises as fs } from "fs";
import path from "path";

const ROOT = process.argv[2] ? path.resolve(process.argv[2]) : process.cwd();
const VISITED = new Set();
const LOCAL_PACKAGES = new Set();

const PKG_FIELDS = [
	"dependencies",
	"devDependencies",
	"optionalDependencies",
	"peerDependencies"
];

async function collectPackageNames(filePath) {
	if (VISITED.has(filePath)) {
		return;
	}
	VISITED.add(filePath);

	const contents = await fs.readFile(filePath, "utf8");
	try {
		const pkg = JSON.parse(contents);
		if (pkg?.name) {
			LOCAL_PACKAGES.add(pkg.name);
		}
	} catch (error) {
		console.warn(`Skipping ${filePath}: ${error.message}`);
	}
}

async function restoreWorkspaceProtocol(filePath) {
	const original = await fs.readFile(filePath, "utf8");
	let pkg;
	try {
		pkg = JSON.parse(original);
	} catch (error) {
		console.warn(`Skipping ${filePath}: ${error.message}`);
		return;
	}

	let changed = false;

	for (const field of PKG_FIELDS) {
		const deps = pkg[field];
		if (!deps) {
			continue;
		}
		for (const [name, version] of Object.entries(deps)) {
			if (!LOCAL_PACKAGES.has(name)) {
				continue;
			}
			if (typeof version !== "string" || version.startsWith("workspace:")) {
				continue;
			}
			deps[name] = "workspace:*";
			changed = true;
		}
	}

	if (changed) {
		const json = JSON.stringify(pkg, null, "\t");
		await fs.writeFile(filePath, `${json}\n`);
		console.log(`Reset workspace deps in ${path.relative(ROOT, filePath)}`);
	}
}

async function walk(dir, visitor) {
	const entries = await fs.readdir(dir, { withFileTypes: true });
	await Promise.all(
		entries.map(async (entry) => {
			if (entry.name === "node_modules" || entry.name === ".git") {
				return;
			}
			const fullPath = path.join(dir, entry.name);
			if (entry.isDirectory()) {
				await walk(fullPath, visitor);
			} else if (entry.isFile() && entry.name === "package.json") {
				await visitor(fullPath);
			}
		})
	);
}

VISITED.clear();
await walk(ROOT, collectPackageNames);

await walk(ROOT, restoreWorkspaceProtocol);