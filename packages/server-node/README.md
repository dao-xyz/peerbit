# Peerbit node
A non-browser node with a CLI and server API so you can manage your non-browser node with easy

## Features
- Request SSL certificate and setup NGINX config so that your node can be accessed from a browser
- Manage topics
- Manage VPC/Networks (add/revoke trust)

## 🚧 WIP 🚧
**This CLI does not work in Windows at the moment. As of now this CLI have only tested with Ubuntu 22.04.**

## Run a node 
Needs port forwarding on 80, 443 (for the console/frontend) and 4002 (for IPFS)

1. 
[Install node version > v.16.15](https://nodejs.org/en/download/package-manager/#debian-and-ubuntu-based-linux-distributions)

(E.g. Ubuntu  19)  
```
curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
sudo apt-get install -y nodejs
```


then install the CLI
```
npm install -g @dao-xyz/peerbit-node    
```


2. 
Start the node in a background process
```sh
peerbit start > log.txt 2>&1 &
```
or if you just want an IPFS node as a relay in a separate process
```
peerbit start --relay  > log.txt 2>&1 &
```
(you can try to run ```docker exec ipfs_host ipfs id)``` to check ipfs is running in the background)

2.
Setup a test domain (so can access the node)

```sh
sudo peerbit domain test --email YOUR_EMAIL 
```

(``sudo`` is needed because docker will be installed if it is not available)

After a while a domain will be written out that you can access and learn more about your peer


3. 
You might want to subscribe to a few topics. If you are working on an app where you rely on some topic, e.g. "world". Then you want to run following commands

~~peerbit topic add "world"~~

~~peerbit topic add "world!"~~

~~peerbit topic add "_block"~~

Latest version `ipfs-http-client` behaves unexpectedly on long lasting pubsub connection. For a more resiliant behaviour you have to subscribe directly with docker 

```
docker exec ipfs_host ipfs pubsub sub "world" &
docker exec ipfs_host ipfs pubsub sub "world!" &
docker exec ipfs_host ipfs pubsub sub "_block" &
```

First topic is the general topic for messages. The second topic ending with "!" is a topic designated for replicators. The last topic is used to distributed IPFS blocks on PubSub. Subscribing for that topic enables you to have Browser to Browser block share without them beeing in the same swarm. 

### More documentation

Run
```sh
peerbit --help
```