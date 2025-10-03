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
Use pnpm for all publishing so workspace dependencies resolve correctly during packaging.

### Stable release
```bash
pnpm run release
```

### Release candidate
```bash
pnpm run release:rc
```

These scripts forward to aegir with the pnpm publish command, so no additional flags are required.


