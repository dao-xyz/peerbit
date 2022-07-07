# orbit-db-identity-provider

[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/orbitdb/Lobby) [![Matrix](https://img.shields.io/badge/matrix-%23orbitdb%3Apermaweb.io-blue.svg)](https://riot.permaweb.io/#/room/#orbitdb:permaweb.io) 
[![npm version](https://badge.fury.io/js/orbit-db-identity-provider.svg)](https://www.npmjs.com/package/orbit-db-identity-provider)

> Default identity provider for OrbitDB

`Identities` is a package to manage identities in [@OrbitDB](http://github.com/orbitdb/orbit-db)

## Table of Contents

  - [Install](#install)
  - [Usage](#usage)
    - [Creating an identity](#creating-an-identity)
    - [Adding a custom identity signer and verifier](#adding-a-custom-identity-signer-and-verifier)
    - [Properties](#properties)
      - [id](#id)
      - [publicKey](#publickey)
      - [signatures](#signatures)
  - [Contribute](#contribute)
    - [Tests](#tests)
    - [Build](#build)
    - [Linting](#linting)
  - [License](#license)

## Install

This project uses [npm](https://npmjs.com/) and [nodejs](https://nodejs.org/)

```sh
$ npm i --save orbit-db-identity-provider
```

## Usage

The `Identity` object contains signatures proving possession of some external identifier and an OrbitDB public key. This is included to allow proof of ownership of an external identifier within OrbitDB.

### Creating an identity
```js
const Identities = require('orbit-db-identity-provider')
const options = { id: 'local-id'}
const identity = await Identities.createIdentity(options)

console.log(identity.toJSON())
// prints
{
  id: '045757bffcc7a4f4cf94c0cf214b3d3547a62195a09588df36b74aff837b2fdc14551360a323bf9de2ac8fb2eda9bd1bae5de53577a8db41ee2b46b4bf8cd7be33',
  publicKey: '04b5c54ef8f2514a58338e64aa08aa6052c3cfef1225a10b51017f2ad63a92fb166e7a19cba44321c9402ab1b62c940cd5e65e81e4d584c1208dbd021f6e22c6f5',
  signatures:  {
    id: '3046022100aab534483f474bd3791eb9dcf1f61b6bdb4b07f70e8eca1ea7b530ac0ca13ca1022100e9d95eeeacc9813808400eb37f8aae6be7873df460d2a03e7a19132e34f0bd16',
    publicKey: '30440220514b6fee38cbec96d9851905e575d6e209834c94be5e009a8261737d4ef23dfc0220794fa8dee564701d337b68fdbeef76bb81d777154c211d84ac345ec287a2a8e1'
  },
  type: 'orbitdb'
}

```
If `options.type` is not specified, Identities will default to creating an identity with type '`orbitdb'`, meaning the signing key will sign another OrbitDB public key. This public key can be an already-existing OrbitDB key allowing you to link several keys to a 'master' OrbitDB key to, for example, link keys across devices.

To use an existing keystore, you can pass it as an argument in the options as follows:
```js
const identity = await Identities.createIdentity({ id: 'local-id', keystore: existingKeystore })
```

#### Creating an identity with a DID
Decentralized Identifiers (DID) is a common way to represent a digital identity. Below is an example using the `did:key` method (specifically [key-did-provider-ed25519](https://github.com/ceramicnetwork/key-did-provider-ed25519)).
```js
const { Ed25519Provider } = require('key-did-provider-ed25519')
const { default: KeyResolver } = require('key-did-resolver')
const Identities = require('orbit-db-identity-provider')
Identities.DIDIdentityProvider.setDIDResolver(KeyResolver.getResolver())

const seed = // 32 bytes of entropy (Uint8Array)
const didProvider = new Ed25519Provider(seed)
const identity = await Identities.createIdentity({ type: 'DID', didProvider })
```

#### Creating an identity with an Ethereum wallet
Identities can also be created using Ethereum wallets. The example below uses [ethers](https://docs.ethers.io/) to open a users wallet and sign the identity.
```js
import Identities from "@dao-xyz/orbit-db-identity-provider";
import { ethers } from "ethers";

const provider = new ethers.providers.Web3Provider(/* window.ethereum */);
const wallet = provider.getSigner();
const identity = await Identities.createIdentity({
  type: "ethereum",
  wallet,
});
```
_Note: If you don't supply a `wallet`, a random one will be created for you._

### Create identity using existing keys

To create an identity using existing keys, you need to install `localstorage-level-migration`

```js
const Identities = require('orbit-db-identity-provider')
const migrate = require('localstorage-level-migration')
const options = { id: 'new-id', migrate: migrate('/path/to/keys') }
const identity = await Identities.createIdentity(options)

console.log(identity.toJSON())
// prints
{
  id: '<new-id>',
  publicKey: '<compressed-original-key>',
  signatures:  {
    id: '<new-id-signed-by-public-key>',
    publicKey: '<public-key-signed-by-id>'
  },
  type: 'orbitdb'
}

```

### Adding a custom identity signer and verifier

To link an OrbitDB signing key with an external identity, you must provide a custom class which implements the `IdentityProvider` [interface](https://github.com/orbitdb/orbit-db-identity-provider/blob/master/src/identity-provider-interface.js).

```js
class MyIdentityProvider extends IdentityProvider {
  static get type () { return 'MyIdentityType' } // return type
  async getId () { } // return identifier of external id (eg. a public key)
  async signIdentity (data) { } //return a signature of data (signature of the OrbtiDB public key)
  static async verifyIdentity (identity) { } //return true if identity.sigantures are valid
}

Identities.addIdentityProvider(MyIdentityProvider)

// to create an identity of type `MyIdentityType`
const identity = await Identities.createIdentity({ type: `MyIdentityType`})

```

### Properties

#### id

Returns the ID of the external identity.

#### publicKey

Returns the signing key used to sign OrbitDB entries.

#### signatures
Returns an object containing two signatures
```js
{ id: <id-signature>, publicKey: <pub-key+id-siganture> }
```

The first signature, `id`, is `identity.id` signed by `identiy.publicKey`. This allows the owner of `id` to prove they own the private key associated with `publicKey`. The second signature `publicKey` is created by signing the concatenation `identity.signature.id + identity.publicKey` using `identity.id`. This links the two identifiers.

## Contribute

Please, feel free to contribute! Take a look at [the issues](https://github.com/orbitdb/orbit-db-identity-provider/issues), and comment on an existing issue or create a new one if you have questions, bugs, or suggestions. For larger PRs, open an issue first if you could - drive-by PRs are also welcomed.

Please abide by the [Code of Conduct](CODE_OF_CONDUCT.md). For more on contributing to [@OrbitDB](https://github.com/orbitdb/), check out the docs in [orbitdb/welcome](https://github.com/orbitdb/welcome).

### Tests

Run tests with:

```sh
$ npm test
```

### Build

The build script will build the distribution file for browsers.

```sh
$ npm run build
```

### Linting

Please use [`standard`](https://standardjs.com). To check,


```sh
$ npm run lint
```

## License

[MIT](LICENSE) © 2018 Haja Networks Oy
