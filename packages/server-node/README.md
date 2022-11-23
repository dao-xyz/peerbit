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

Documentation
```sh
peerbit --help
```
