# Findings (2026-02-06)
- `queryAll` uses RPC default timeout of 10s; search/iterate does not override this even when `remote.wait.timeout` is much longer. Under churn, RPC requests can time out before `waitForResolved` deadlines, leading to missing responses that are silently swallowed unless `throwOnMissing` is set. This can cause partial results to be treated as complete. (see `packages/programs/rpc/src/controller.ts` and `packages/programs/data/document/document/src/search.ts`)
- `MissingResponsesError` is caught in `queryCommence` and only logged unless `remote.throwOnMissing` is set, so caller has no signal to keep iterator open or requery missing peers.

# Changes Applied
- `queryCommence` now aligns RPC timeout with `remote.wait.timeout` when provided, reducing premature MissingResponsesError during churn.
- `MissingResponsesError` now carries `missingGroups` metadata and `queryCommence` exposes this via an `onMissingResponses` callback.
- `iterate` marks the initial fetch as incomplete when missing responses are detected to avoid prematurely closing the iterator.

# Tests
- Attempted: `node ./node_modules/aegir/src/index.js run test --roots ./packages/programs/rpc -- -t node --grep "queryAll"`
- Result: failed (missing local dependency `node_modules/aegir`)
