import { Peerbit } from "peerbit";

const peerA = await Peerbit.create();
const peerB = await Peerbit.create();

// this address was obtained by deploying a server node using the CLI

const relayAddress =
	"/dns4/8d2cc94505a712420a3e84cb56c4e15c7dc9eae6.peerchecker.com/tcp/4003/wss/p2p/12D3KooWERXVxavXvounVAyEETdGafASHKDNvWeeLCmALJ8esrqo";

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
