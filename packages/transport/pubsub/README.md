# Direct Sub

Pubsub protocol built on top of [Direct Stream](./../stream/README.md)

Features
- Accurate
	
	```typescript
	.getSubscribers(topic)
	``` 
	
	method based on aggregated subscriber info, not only from immediate peers.

- Subscriptions with associated metadata. E.g. you can subscribe to topics and provide data that explains the purpose for peers
	```typescript 
	.subscribe(topic, new UInt8Array([1,2,3]))
	```

	```typescript
	.getSubscribersWithData(topic, data)
	``` 

- Efficient content routing through path-finding algorithms
	```typescript
	.publish(data, { topics: ["a","b"]})
	```
	will try to find the subscribers of "a" and "b" and send messages with the shortest path in the network.
