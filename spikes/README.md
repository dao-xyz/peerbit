# spikes/ — throwaway feasibility artifacts

Everything under `spikes/` is a **throwaway feasibility spike**, not production code
and not part of any build.

- These crates/scripts are **intentionally isolated** from the repo's build graph:
  - There is no root `Cargo.toml [workspace]` in this repo (every Rust crate is
    standalone), so a spike crate with its own `Cargo.toml` is never pulled into a
    cargo workspace.
  - `spikes/` is **not** listed in `pnpm-workspace.yaml` or any `package.json`
    `workspaces` field, so `pnpm` at the repo root never installs or builds it.
  - CI globs target `packages/**` / `apps/**`, not `spikes/**`.
- Favor **learning over polish**. Code here exists to answer a yes/no feasibility
  question with maximum signal, then be deleted or promoted deliberately.
- Nothing here is imported by any shipping package.

## Contents

- `rust-libp2p-poc/` — Does a `rust-libp2p` node swarm interoperate with the existing
  Peerbit `js-libp2p` fleet on node peers (the "replace js-libp2p on node peers"
  hypothesis)? See that crate's `README.md` for the verdict and how to reproduce.
