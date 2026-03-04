# Managing remote nodes
The CLI allows you to connect to and manage multiple remote nodes simultaneously.


## Linking remote nodes
To connect to remote nodes, you need to instruct the CLI on how to establish a connection with them and optionally assign them to specific groups. Groups facilitate convenient connections to subsets of your nodes.


### Link a new remote
```sh
peerbit remote add <name> <address>
```

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

### Non-interactive remote automation (CI-friendly)

For CI/CD and scripts, you can run update/export commands without entering the
interactive `remote connect` shell.

Update selected remotes to a new `@peerbit/server` version and wait until they
come back online:

```sh
peerbit remote self-update 5.10.14 --group bootstrap --directory ~/.peerbit
```

Export preferred bootstrap multiaddrs for selected remotes (one multiaddr per
line, directly usable as `bootstrap-4.env` content):

```sh
peerbit remote export-bootstrap --group bootstrap --directory ~/.peerbit
```

JSON export variant:

```sh
peerbit remote export-bootstrap --group bootstrap --json --directory ~/.peerbit
```


### Allow more machines to access your remote nodes
By default, when you spawn nodes using the [CLI](/modules/deploy/server/automatic.md), permissions are granted to your local machine so that your local machine can access the spawned remote nodes. This is achieved by keeping a record on the remote nodes that your local machine's public key as permitted to perform admin actions. To prove authorization, every request from your local machine to the remote nodes is signed by your private key.

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
