# Spawn nodes with the CLI

The CLI offers the capability to directly spawn nodes within server centers, presently limited to AWS, using a set of commands.

## AWS

1. 

Configure your AWS environment to ensure the availability of a .aws configuration folder in your system's home directory. If you haven't already done this, consult this [guide](https://wellarchitectedlabs.com/common/documentation/aws_credentials/) if you have not done it already. 

2. 

Install the CLI locally:
```sh
npm install -g @peerbit/server
```

3. 

Initiate the spawn command:
This will create a new node in your default region.
```sh
peerbit remote spawn aws --count 1
```
Please note that this process might require several minutes.


For additional spawning options, refer to:
```sh
peerbit remote spawn aws --help
```

4. 

Verify the status of your node:
```sh
peerbit remote ls
```

If you have recently spawned the node and it displays as offline, it's possible that you need to wait a bit longer for the SSL certificates and configurations to finalize. This step typically consumes the most time.

You can also access your AWS console within the region where the deployment was initiated to monitor the progress.


## Other cloud vendors
If you would like to see support for specific cloud vendors, please suggest them [here](https://github.com/dao-xyz/peerbit/issues).