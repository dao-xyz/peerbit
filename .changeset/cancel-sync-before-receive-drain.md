---
"@peerbit/shared-log": patch
---

Cancel built-in synchronizer dispatch before close/drop drains admitted receives, so an abort-aware response shipment cannot block the shutdown that must cancel it. Preserve final storage teardown and physical-work accounting until the receives finish; existing custom synchronizers remain compatible through an optional cancellation hook.
