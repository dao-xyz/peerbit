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


### Allow more machines to access your remote nodes
Remote administration is authorized with Peerbit identities. Each request is
signed by your private key, and the server checks the corresponding public key
against its trust list. Grant the identity you use for administration when you
start the server, or add it from an already trusted session as described below.

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

