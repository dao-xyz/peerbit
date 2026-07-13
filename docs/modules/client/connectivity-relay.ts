import { Peerbit } from "peerbit";

const peerA = await Peerbit.create();
const peerB = await Peerbit.create();

// Supply the complete advertised multiaddr from your relay. A managed hostname
// obtained with `peerbit domain lease claim` has the form
// `p-<20 hex>.nodes.peerchecker.com`; the multiaddr also contains the relay's
// own peer ID.
const relayAddress = process.env.PEERBIT_RELAY_MULTIADDR;
if (!relayAddress) {
	throw new Error("Set PEERBIT_RELAY_MULTIADDR to your relay's advertised address");
}

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
