# Running a server node 
A non-browser node with a CLI and server API so you can manage your non-browser node with easy

## Features
- Request SSL certificate and setup NGINX config so that your node can be accessed from a browser

**This CLI does not work in Windows at the moment. As of now this CLI have only tested with Ubuntu 22.04.**

## Run a node 
Needs port forwarding on 80, 443 (for the console/frontend) and 4002 (for Libp2p)

1. 
[Install node version > v.16.15](https://nodejs.org/en/download/package-manager/#debian-and-ubuntu-based-linux-distributions)

(E.g. Ubuntu  19)  
```
curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
sudo apt-get install -y nodejs
```


then install the CLI
```
npm install -g peerbit-node    
```


2. 
Start the node in a background process
```sh
peerbit start > log.txt 2>&1 &
```

This will start a peerbit client

3.
Setup a test domain (so can access the node). The command below might take a while to run.

```sh
sudo peerbit domain test --email YOUR_EMAIL 
```

(``sudo`` is needed because docker will be installed if it is not available)

After a while a domain will be written out that you can access and learn more about your peer


4. 
If you ssh'ed into a some server, remember to do this before exiting the terminal:

Remove all jobs from the shell and make them ignore SIGHUP

```sh
disown -ah
```



### More documentation

Run
```sh
peerbit --help
```

Or for a specific command, e.g. 

Run
```sh
peerbit start --help
```
