---
"@peerbit/server": patch
"@peerbit/test-utils": patch
---

Replace the terminal table renderer with cli-table3 so server remote listings and memory reports no longer bring in tty-table's unused CSV parser dependency chain. Preserve table fields, multiline values and ANSI-colored content without changing networking or authorization behavior.

Wrap long values to the terminal width without truncating ANSI-colored content, using wrap-ansi. Exclude compiled server tests from published tarballs while retaining the source tests.
