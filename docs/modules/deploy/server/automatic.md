# Provision a server

The Peerbit CLI no longer creates virtual machines or DNS records. The old
provider-specific provisioning commands depended on a hosted test-domain
service, so they could not produce a working, portable deployment after that
service was retired.

To deploy a public node:

1. Create a server with the provider of your choice.
2. Point a domain you control to the server.
3. Follow the [existing-server guide](./custom.md) to configure TLS and start
   Peerbit.
4. Register the server from the same local identity that the setup guide grants
   with `peerbit remote add <name> <address>`.

Existing cloud resources are not deleted by this change. Manage them in their
provider console. `peerbit remote terminate` remains available for legacy
Hetzner entries; legacy AWS entries produce an explicit message containing the
instance ID and region so they can be cleaned up manually. Termination
preflights the selected entries: if an AWS entry is selected, no Hetzner entry
is deleted. Clean up the AWS instance and remove its local entry first, or name
only the Hetzner entries you intend to terminate.
