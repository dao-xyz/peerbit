# Deployment

## Serverless
Since Peerbit at the current stage is only javascript modules, you can deploy your project to any package manager, like NPM or Github Packages, in order to import them into your app directly, like a react project or electron app.

## Server
Sometimes it make sense to deploy a Peerbit on a server that can be accessed through a domain. There are mainly two reasons why you want to do this: 
- Hole punching. Two browser can not connect to each other directly, and requires an intermediate that enables a direct connection to be made (if possible)
- A replicator that is always online. While a clients can store data themselves, sometimes you need to be sure that there is always one node online. 

To deploy a server node, there is a handy CLI. [See this](./server-node.md)