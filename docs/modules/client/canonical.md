# Canonical runtime + proxy clients

Peerbit can run selected programs in a single “canonical” host (SharedWorker, ServiceWorker, parent window, server) and let multiple “edge” contexts open proxies that share (most of) the same public API.

The core idea:
- The **host** runs one real Peerbit instance and a module registry (`@peerbit/canonical-host`).
- The **client** connects to the host (`@peerbit/canonical-client`) and can open module channels.
- **Program-specific adapters** live in the program packages (e.g. `@peerbit/document-proxy`, `@peerbit/shared-log-proxy`) and let `peer.open(...)` “morph” into proxies automatically.

Notes:
- You only need to register modules for programs you want to open directly; some modules embed subservices (e.g. `Documents` exposes `docs.log`) without requiring separate module registration.

## Example types

[types.ts](./canonical-types.ts ':include :type=code')

## Host: SharedWorker

[worker.ts](./canonical-host-shared-worker.ts ':include :type=code')

## Host: ServiceWorker

[service-worker.ts](./canonical-host-service-worker.ts ':include :type=code')

## Host: Window / iframe

[parent.ts](./canonical-host-window.ts ':include :type=code')

## Client: iframe / child window

[iframe.ts](./canonical-client-window.ts ':include :type=code')

## Client: “full magic” open (recommended)

Write app/program code against `*Like` interfaces so it works for both local and proxy instances.

[app.ts](./canonical-client-auto-open.ts ':include :type=code')

Notes:
- In the canonical-only client model, `peer.open(...)` always goes through registered canonical adapters.
- Opening by address (string) is supported: `peer.open("bafy...")` asks the host to `blocks.get(...)` the program bytes and then deserializes it locally before selecting an adapter.
  - The program class must be loaded in the client bundle (same requirement as `Program.load(...)`).
  - If the program requires `args` at open-time (e.g. `Documents` needs `{ type }`), call `peer.open<Documents<T>>(address, { args: { type: T } })`.
