# Direct Sub

Pubsub protocol built on top of [Direct Stream](./../stream/README.md)

Features
- Accurate
	
	```typescript
	.getSubscribers(topic)
	``` 
	
	method based on aggregated subscriber info, not only from immediate peers.


- Efficient content routing through an approximate path finding algorithm
	```typescript
	.publish(data, { topics: ["a","b"]})
	```
	will try to find the subscribers of "a" and "b" and send messages with the shortest path in the network.

- Packet prioritization. Data packages can be sent with different priority. That allows some traffic to pass through the traffic faster than other. This is useful in case of congestion
- Mode'd delivery. Packages can be sent with different delivery mode 'Acknowledge' (wait for acknowledgement from all known subscribers), 'Seek' (find new subscribers for a topic), 'Silent' (just deliver).
- Redundancy in data delivery. Data packages can be sent with different redundancy. This means that you can choose to send some packages only in one path (the fastest) or send a package in the 'N' fastest paths (this gives you a redundancy degree of 'N'). This feature is useful when you want to adapt delivery for unstable networks or when you want to make sure that some messages are delivered with high probability (without waiting for Acknowledgemts and retry)


Protocol specification (TODO)

Currently the protocol itself is not specificed more than as in this implementation with messages types found [here](https://github.com/dao-xyz/peerbit/blob/master/packages/transport/stream-interface/src/messages.ts) and [here](https://github.com/dao-xyz/peerbit/blob/master/packages/transport/pubsub-interface/src/messages.ts). Message handling logic can be found [here](https://github.com/dao-xyz/peerbit/blob/aa577a5e5b2b4920662de5e0efb92fb97a5dc63c/packages/transport/pubsub/src/index.ts#L502) and [here](https://github.com/dao-xyz/peerbit/blob/aa577a5e5b2b4920662de5e0efb92fb97a5dc63c/packages/transport/stream/src/index.ts#L1057).
