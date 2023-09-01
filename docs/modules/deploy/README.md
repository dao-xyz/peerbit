## Deployment Options with Peerbit

### Serverless Deployment
As Peerbit currently revolves around JavaScript modules, you have the flexibility to deploy your projects using various package managers such as NPM or Github Packages. By doing so, you can effortlessly incorporate them directly into your applications. This approach works seamlessly with projects like React applications or Electron apps.

### Server Deployment
In certain scenarios, deploying a Peerbit instance on a server accessible via a domain is beneficial. This choice is driven by a couple of key reasons:

1. **Hole Punching**: Direct browser-to-browser connections require an intermediary peer to facilitate communication. This peer assists browser clients in discovering and connecting to others effectively. While there exist a [bootstrapping network](/modules/client/?id=bootstrapping) that you can use for hole punching, some apps might require dedicated nodes to relay data when direct connections are not possible.

2. **Persistent Replicator**: While browser clients can store data on their own, there are instances where maintaining an always-online node is crucial.

For deploying a server node, Peerbit offers a convenient CLI. Further details can be found [here](/modules/deploy/server/).