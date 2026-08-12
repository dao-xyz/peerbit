---
"@peerbit/string": major
---

Breaking type-surface change inherited from @peerbit/shared-log: the string store's public `Args` embeds `SharedLogOptions`, whose `compatibility` option was removed. Opening a string store with an explicit `compatibility` value now rejects at open (`CompatibilityModeRetiredError`); omit the option to keep current behavior.
