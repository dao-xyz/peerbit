---
"@peerbit/document": patch
---

Preserve caller-provided `remote.from` targeting on the iterator's first fetch

`fetchFirst` spread `options.remote` but then overwrote `from` with
`fetchOptions?.from ?? initialRemoteTargets`, both of which are undefined on
the first fetch unless `reach.discover` is used. The explicit
`from: undefined` clobbered the caller's targeting hint, so `queryCommence`
fell through to `getCover` and the cold-start fallback, querying connected
peers that may never respond (e.g. a relay that does not run the program) and
stalling the first batch until the full remote timeout. Caller-provided
`remote.from` is now used as the fallback; internal callers passing
`fetchOptions.from` (missing-response retries, late-join refetches) and
`reach.discover` targets keep precedence.
