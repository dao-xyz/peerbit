# Agents

## Building
```bash 
pnpm run build 
```

## Testing 
```bash
pnpm run test
```

Run specific test file
```bash
node ./node_modules/aegir/src/index.js run test \
  --roots ./packages/programs/data/shared-log -- -t node \
  --grep "will prune on put 301 after join"
```

## Releasing
Versioning is handled by [changesets](https://github.com/changesets/changesets).
Author a changeset for any PR that changes a publishable package:
```bash
pnpm changeset
```
Merging the bot's "chore: version packages" PR bumps versions and publishes to
npm automatically. See [RELEASING.md](./RELEASING.md) for the full flow.

Use pnpm for all publishing so workspace dependencies resolve correctly during packaging.

### Stable release
```bash
pnpm run release
```
Publishes every public package whose `package.json` version is not yet on npm
(`from-package`). This is the command the release workflow runs after the
version PR is merged; run it locally only as a manual fallback.

### Release candidate
```bash
pnpm run release:rc
```

`release:rc` forwards to aegir with the pnpm publish command, so no additional flags are required.


