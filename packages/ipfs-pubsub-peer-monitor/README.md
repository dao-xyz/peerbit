# ipfs-pubsub-peer-monitor

> Know when peers are joining and leaving an IPFS PubSub topic

`ipfs-pubsub-peer-monitor` listens on a [IPFS PubSub](https://github.com/ipfs/interface-ipfs-core/blob/master/SPEC/PUBSUB.md) topic and emits an event when a peer joins or leaves the topic.

This module is based on [ipfs-pubsub-room](https://github.com/ipfs-shipyard/ipfs-pubsub-room) that can provide the same functionality. It contains extra features that are not necessary for purely wanting to know joins/leaves, so this module was created to do that and only that.

## Usage

```js
const PeerMonitor = require('ipfs-pubsub-peer-monitor')

// Get an IPFS instance somehow
const ipfs = ...

// Topic to monitor
const topic = 'abc'

// Make sure to subscribe to the channel before monitoring it!
ipfs.pubsub.subscribe(topic, (message) => {}, (err, res) => {})

// Pass an IPFS pubsub object and the topic to the monitor
const topicMonitor = new PeerMonitor(ipfs.pubsub, topic)

// When a peer joins the topic
topicMonitor.on('join', peer => console.log("Peer joined", peer))
topicMonitor.on('leave', peer => console.log("Peer left", peer))
topicMonitor.on('error', e => console.error(e))
```
