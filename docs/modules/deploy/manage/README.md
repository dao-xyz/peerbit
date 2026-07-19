# Managing remote nodes

The CLI allows you to connect to and manage multiple remote nodes simultaneously.

## Linking remote nodes

To connect to remote nodes, you need to instruct the CLI on how to establish a connection with them and optionally assign them to specific groups. Groups facilitate convenient connections to subsets of your nodes.

### Link a new remote

```sh
peerbit remote add <name> <address> --peer-id <server-peer-id>
```

Use the peer ID printed by the server and verify it out of band. If
`--peer-id` is omitted, enrollment uses trust on first use (TOFU): the CLI pins
the identity returned by that TLS endpoint and prints it for verification.
An existing remote name keeps its pin across re-enrollment; an unexpected
identity change is rejected without modifying the saved record. To rotate a
server identity, remove the old remote and add it again only after verifying
the replacement peer ID out of band.

### View nodes and their statuses

```sh
peerbit remote ls
```

### Connecting to nodes so you can perform actions on them

```sh
peerbit remote connect YOUR_NAMED_NODE
```

or for a group

```sh
peerbit remote connect --group GROUP_NAME
```

### Allow more machines to access your remote nodes

Remote administration is authorized with Peerbit identities. Each request is
signed by your private key, and the server checks the corresponding public key
against its trust list. Requests also carry a short-lived timestamp, a
single-use random nonce, the pinned server peer ID, and a per-process server
boot ID. This prevents a captured request from being reused later, after a
restart, or against another server. Grant the identity you use for
administration when you start the server, or add it from an already trusted
session as described below.

Clients also freshness-check the server-signed authentication descriptor. Keep
administrator and server clocks synchronized; a stale cached or replayed
descriptor is rejected during discovery.

Authenticated management request bodies are limited to 64 MiB and are checked
against the exact signed byte length and SHA-256 digest before they are decoded.

If you need to modify permissions for which nodes that can perform actions, do follow these steps:

1.

Go to the machine which you want to add and learn its publickey by invoking

```sh
peerbit id
```

2.

Get access to the nodes you want to modify directly. In their terminals run:

```sh
peerbit remote connect
```

OR

Connect to the nodes you want to modify (see previous section)

3.

To give a peer-id admin capabilities

```sh
access grant <peer-id>
```

To revoke admin capabilities from a peer-id

```sh
access deny <peer-id>
```

Where <peer-id> is the id you obtained in step 1.
