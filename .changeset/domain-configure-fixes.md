---
"@peerbit/server": patch
---

Fixes for `peerbit domain configure`, which `peerbit domain lease claim --configure` also runs:

- The UI, readiness token and NGINX config that configure stages for the new container now get folder mode 0755 and file mode 0644. They were created under the caller's umask, and the copied UI kept the modes of the installed package, so under a restrictive umask such as 077 or 027 NGINX's worker processes could not read them. HTTPS then answered 403 or 404, the readiness check failed for 5 minutes, and configure rolled back.
- The readiness check now gives up after three 403 or 404 answers in a row from the new container, so configure rolls back within seconds instead of after 5 minutes when NGINX cannot serve the staged files. Connection errors, TLS errors and other answers are still retried until the deadline, so a first-time setup, whose HTTPS port stays closed until its certificate is issued, waits as before.
