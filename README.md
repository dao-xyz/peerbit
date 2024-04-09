<br>
<p align="center">
    <img width="140" src="./docs/peerbit-logo.png"  alt="Peerbit icon Icon">
</p>

<h1 align="center" style="font-size: 5vmin;">
    <strong>
        Peerbit
   </strong>
</h1>

<h3 align="center">
    Develop for a distributed web with Peerbit
</h3>

<h3 align="center" >E2EE &nbsp; &nbsp; P2P &nbsp; &nbsp; Auto-sharding  &nbsp; &nbsp;  Searchable</h3>

<h3 align="center"><a href="https://peerbit.org">Documentation</a> - <a href="https://github.com/dao-xyz/peerbit-examples">Examples</a> - <a href="https://matrix.to/#/#peerbit:matrix.org">Chat on Matrix</a></h3>

## A building block for the decentralized web

Peerbit is as easy-to-use as Firebase and provide P2P functionality like OrbitDB or GunJS yet with performance for data-intensive applications like live-streaming and cloud-gaming. It’s built on top of Libp2p (and works with IPFS) supporting encryption, sharding and discoverability (searching).

Your database schema can remain very simple but still utilize P2P networks, auto-scaling, E2E-encryption, discoverability and all other features you’d expect from a database.

## Optimized for performance

Peerbit is performant, so performant in fact you can use it for [streaming video](https://stream.dao.xyz) by having peers subscribing to database updates. In a low latency setting, you can achieve around 1000 replications a second and have a thoughput of 100 MB/s.

![Dogestream](/docs/videostream.gif)

## Other examples

### [Chat room](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/one-chat-room/)

[<img src="https://github.com/dao-xyz/peerbit-examples/blob/master/packages/one-chat-room/demo.gif" width="600" />](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/one-chat-room/)

### [Lobby + chat rooms](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/many-chat-rooms/)

[<img src="https://github.com/dao-xyz/peerbit-examples/blob/master/packages/many-chat-rooms/demo.gif" width="600" />](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/many-chat-rooms/)

## [Blog platform](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/blog-platform/)

[<img src="https://github.com/dao-xyz/peerbit-examples/blob/master/packages/blog-platform/demo-cli.gif" width="600" />](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/blog-platform/)


### [Sync files](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/file-share/)

#### [React app](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/file-share/)

[<img src="https://github.com/dao-xyz/peerbit-examples/blob/master/packages/file-share/demo-frontend.gif" width="600" />](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/file-share/)

#### [CLI](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/file-share/)

[<img src="https://github.com/dao-xyz/peerbit-examples/blob/master/packages/file-share/demo-cli.gif" width="600" />](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/file-share/)

### [Collaborative machine learning](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/collaborative-learning/)

[<img src="https://github.com/dao-xyz/peerbit-examples/blob/master/packages/collaborative-learning/demo.gif" width="600" />](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/collaborative-learning/)

## Get Started

1. Install Peerbit by following the simple setup instructions in our [Installation Guide](https://peerbit.org/#/getting-started).

2. Dive into our comprehensive [Documentation](https://peerbit.org/#/modules/client/) or checkout the [Example repository](https://github.com/dao-xyz/peerbit-examples) to explore the powerful features and learn how to leverage Peerbit to its fullest potential.

3. Join us on [Matrix](https://matrix.to/#/#peerbit:matrix.org) to connect, share ideas, and collaborate with like-minded individuals.

## Contribute

Peerbit is an open-source project, and we welcome contributions from developers like you! Feel free to contribute code, report issues, and submit feature requests. Together, let's shape the future of Peerbit.

IMPORTANT: Peerbit uses yarn.

1. Check yarn version: `yarn -v` should print something
2. Install: `yarn`
3. Build: `yarn build`
4. Run tests: `yarn test` in root in a specifc subpackage

You might possibly need to CMD + Shift + P and then enter to restart the typescript server after the build step.

To create a new package, follow the following steps:

1. Clone the time folder within /packages/utils/time to the desired destination and rename it
2. Update the package.json `name`, `description`, `version` fields
3. Possibly add other depencencies to the package.json `dependencies` field (like `@peerbit/crypto`)
4. Delete contents in CHANGELOG.md
5. Update the root package.json `workspaces` field
6. Update root lerna.json `workspaces` field
7. run yarn once in root

We recommend running tests with the VS Code integration though: https://github.com/CoderLine/mocha-vscode

## Let's Get Coding!

[peerbit.org](https://peerbit.org)
