# Run Peerbit on existing server

With the CLI you can

- Request SSL certificate
- Setup NGINX config so that your node can be accessed from a browser

**This CLI does not work in Windows at the moment. As of now this CLI has only tested with Ubuntu 22.04.**

## Server configuration 
Needs port forwarding on 
- 80, 443 (for the console/frontend) 
- 4002-4005 (for Libp2p transports)
- 9002 for accessing the server API remotely


## Installation and setup 


1. 

[Install node version > v.16.15](https://nodejs.org/en/download/package-manager/#debian-and-ubuntu-based-linux-distributions)

(E.g. Ubuntu  19)  
```
curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
sudo apt-get install -y nodejs
```


then install the CLI
```
npm install -g @peerbit/server
```

2. *Skip this step if testing locally*

Setup a test domain (so can access the node). The command below might take a while to run.

```sh
sudo peerbit domain test --email YOUR_EMAIL 
```

(``sudo`` is needed because docker will be installed if it is not available)

After a while, a domain will be written out that you can access to learn more about your peer.

3. 
Start the node in a background process
```sh
peerbit start > log.txt 2>&1 &
```

This will start a peerbit client
 
4. *Skip this step if testing locally*

If you ssh'ed into some server, remember to do this before exiting the terminal:

Remove all jobs from the shell and make them ignore SIGHUP

```sh
disown -ah
```



#### More documentation

Run
```sh
peerbit --help
```

Or for a specific command, e.g. 

Run
```sh
peerbit start --help
```
