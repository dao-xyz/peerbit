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
  --grep "will prune on put 300 joining on insertion concurrently"
```

