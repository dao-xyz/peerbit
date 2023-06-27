# Deployment

## Serverless
Since Peerbit at the current stage is only javascript modules, you can deploy your project to any package manager, like NPM or Github Packages, in order to import them into your app directly, for example a React project or Electron app.

## Server
Sometimes it make sense to deploy a Peerbit on a server that can be accessed through a domain. There are mainly two reasons why you want to do this: 
- Hole punching. Two browser can not connect to each other directly without the aid on an intermediate peer that allows the browser clients to find other
- A replicator that is always online. While a client in the browser can store data themselves, sometimes you need to be sure that there is always one node online. 

To deploy a server node, there is a handy CLI. [See this](./server-node.md)