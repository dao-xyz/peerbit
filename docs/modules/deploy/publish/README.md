# Deploy to remote nodes

The CLI offers a straightforward method to deploy your Peerbit programs and facilitate updates with ease!

## Deploying Node Projects

1. Build and package your project:

```sh
npm pack
```

This will generate a file in your current directory with a '.tgz' extension.

2. Connect to the remote node where you intend to deploy. For further details, refer to [this link](/modules/deploy/manage/).

3. Once connected to the node as mentioned in step 2, deploy your packaged build using the following command:

```sh
install the-name-of-your-build.tgz
```

4. The output of the preceding command will display the programs you now have at your disposal. To launch one of them, simply use the command:

```sh
program open --variant PROGRAM_NAME
```

## Overview of How the Package Distribution Works

### Setup

- Each developer has a key obtained when using the CLI for the first time.
- The node is protected with identity authentication, meaning any activity performed on the server first needs to pass the authentication filter.

<p align="center">
    <img width="800" src="./modules/deploy/publish/p1.svg" alt="p1">
</p>

### Distributing a package

- The developer signs the package using their private key and sends it to the node to be updated.

<p align="center">
    <img width="800" src="./modules/deploy/publish/p2.svg" alt="p1">
</p>

### Identity authentication

- Identity authentication verifies that the ID of the sender is trusted to perform actions on the server.

<p align="center">
    <img width="600" src="./modules/deploy/publish/p3.svg" alt="p1">
</p>

You can modify this list using the CLI. For further details, refer to [this link](/modules/deploy/manage/).

### When activity is approved

- The package is installed on the server and is ready to be opened using the `program open` command.

<p align="center">
    <img width="800" src="./modules/deploy/publish/p4.svg" alt="p1">
</p>



### Deploying to many nodes at once

- You can manage more than one node at once by simply [connecting to a group](/modules/deploy/manage/?id=connecting-to-nodes-so-you-can-perform-actions-on-them) of nodes and perform one action.  

- Following command would represent the illustration below

```sh
peerbit remote connect --group X
install ./my-package.tgz
```

<p align="center">
    <img width="800" src="./modules/deploy/publish/p5.svg" alt="p1">
</p>



