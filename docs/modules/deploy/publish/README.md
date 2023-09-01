# Deploy to remotes
The CLI offers a straightforward method to deploy your Peerbit programs and facilitate updates with ease!


## Deploying a Node projects
1. 

Build and package your project:
```sh
npm pack
```
This will generate a file in your current directory with a '.tgz' extension.

2. 

Connect to the remote node where you intend to deploy:

For further details, refer to [this link](/modules/deploy/manage/)

3. 

Once connected to the node as mentioned in step 2, deploy your packaged build using the following command:

```sh
install the-name-of-your-build.tgz
```

4. 
The output of the preceding command will display the programs you now have at your disposal. To launch one of them, simply use the command:

```sh
program open --variant PROGRAM_NAME
```

