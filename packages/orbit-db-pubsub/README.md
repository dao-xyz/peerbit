# orbit-db-pubsub

[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/orbitdb/Lobby) [![Matrix](https://img.shields.io/badge/matrix-%23orbitdb%3Apermaweb.io-blue.svg)](https://riot.permaweb.io/#/room/#orbitdb:permaweb.io) [![Discord](https://img.shields.io/discord/475789330380488707?color=blueviolet&label=discord)](https://discord.gg/cscuf5T)
[![npm version](https://badge.fury.io/js/orbit-db-pubsub.svg)](https://badge.fury.io/js/orbit-db-pubsub)

> Message propagation module for orbit-db

Default message propagation service for [orbit-db](https://github.com/orbitdb/orbit-db). Uses [IPFS](https://dist.ipfs.io/go-ipfs/floodsub-2) [pubsub](https://github.com/ipfs/go-ipfs/blob/master/core/commands/pubsub.go#L23).

# Install

This project uses [npm](http://npmjs.com/) and [nodejs](https://nodejs.org/).

```sh
npm install orbit-db-pubsub
```

## Usage

### API

#### subscribe(topic, onMessageHandler, onNewPeerHandler)

Listen for new messages in `topic`

`onMessageHandler` gets called when a message is received with signature `(topic, data)`

`onNewPeerHandler` gets called when a new peer joins with signature `(topic, peer)`

Returns a promise.

#### unsubscribe(topic)

Stop listening for new messages in `topic`

Returns a promise.

#### disconnect ()

Stop listening for new messages in all topics

Returns a promise.

#### publish(topic, data)

Publish `data` to a `topic`

## Contributing

If you think this could be better, please [open an issue](https://github.com/orbitdb/orbit-db-pubsub/issues/new)!

Please note that all interactions in [@orbitdb](https://github.com/orbitdb) fall under our [Code of Conduct](CODE_OF_CONDUCT.md).

## License

[MIT](LICENSE) ©️ 2016-2018 Protocol Labs Inc., Haja Networks Oy
