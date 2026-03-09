import fs from "node:fs";
import path from "node:path";

const ensure = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

const readJson = (file) => JSON.parse(fs.readFileSync(file, "utf8"));

const writeJson = (file, value) => {
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
};

const updateServerDependency = (bootstrapRoot, version) => {
  const packageJsonFile = path.join(bootstrapRoot, "package.json");
  const packageJson = readJson(packageJsonFile);
  ensure(packageJson.dependencies, `Missing dependencies in ${packageJsonFile}`);
  packageJson.dependencies["@peerbit/server"] = `^${version}`;
  writeJson(packageJsonFile, packageJson);
};

const updateRolloutConfig = (bootstrapRoot, version) => {
  const rolloutFile = path.join(bootstrapRoot, "rollouts", "bootstrap-5.json");
  if (!fs.existsSync(rolloutFile)) {
    console.warn(`Skipping bootstrap rollout sync because ${rolloutFile} does not exist yet`);
    return false;
  }
  const rollout = readJson(rolloutFile);
  rollout.targetVersion = version;
  writeJson(rolloutFile, rollout);
  return true;
};

const run = () => {
  const bootstrapRootArg = process.argv[2];
  const versionArg = process.argv[3];
  ensure(
    typeof bootstrapRootArg === "string" && bootstrapRootArg.length > 0,
    "Usage: node tools/sync-bootstrap-rollout.mjs <bootstrap-root> <server-version>",
  );
  ensure(
    typeof versionArg === "string" && versionArg.length > 0,
    "Missing server version argument",
  );

  const bootstrapRoot = path.resolve(process.cwd(), bootstrapRootArg);
  ensure(fs.existsSync(bootstrapRoot), `Missing bootstrap repo: ${bootstrapRoot}`);

  const rolloutUpdated = updateRolloutConfig(bootstrapRoot, versionArg);
  if (!rolloutUpdated) {
    return;
  }
  updateServerDependency(bootstrapRoot, versionArg);
};

run();
