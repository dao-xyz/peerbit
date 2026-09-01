---
"@peerbit/indexer-sqlite3": patch
---

Prevent admitted scope and index initialization from racing SQLite shutdown by sealing lifecycle admission, binding nested work to ancestor startup, and draining registered startup before closing the database.

Work submitted after shutdown begins now rejects with `NotStartedError`. A `drop()` that conflicts with an already-running `stop()` rejects the same way because the native adapter cannot delete its database after the stop closes the handle.
