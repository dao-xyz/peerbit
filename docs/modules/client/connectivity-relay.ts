import { Peerbit } from "peerbit";

const peerA = await Peerbit.create();
const peerB = await Peerbit.create();

// this address was obtained by deploying a server node using the CLI

const relayAddress =
	"/dns4/b7bb11f066b285b3ac8cca2cc5030a0094293f05.peerchecker.com/tcp/4003/wss/p2p/12D3KooWBKx9dtKCSy2j1NpdFMSDQcQmRqTee8wSetuMZhSifAPs";

/* 
// To test locally can also do
const multaddrs: Multiaddr[] = peer2.libp2p.getMultiaddrs();
const relay = await Peerbit.create()
relayAddress = relay.libp2p.getMultiaddrs()[0].toString()
 */

/* 
┌─┐    
│A│    
└△┘    
┌▽────┐
│Relay│
└△────┘
┌▽┐    
│B│    
└─┘    

 */

await peerA.dial(relayAddress);
await peerB.dial(relayAddress);

// Connected (in-directly)!

await peerA.stop();
await peerB.stop();
