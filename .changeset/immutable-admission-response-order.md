---
"@peerbit/document": patch
---

Check all returned immutable document contexts before admitting a put, so an empty, newer, or same-head reply cannot hide an older conflicting value from another response. Preserve existing timestamp ties, dependency-pointer rules, lookup coverage, and timeouts. This corrects admission of known conflicts; it does not revalidate previously admitted entries or change the wire or storage format.
