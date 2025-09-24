# Agents

## Building
```bash 
yarn build 
```

## Testing 
```bash
yarn test
```

Run specific test file
```bash
node ./node_modules/aegir/src/index.js run test \
  --roots ./packages/programs/data/shared-log -- -t node \
  --grep "will prune on put 300 joining on insertion concurrently"
```


