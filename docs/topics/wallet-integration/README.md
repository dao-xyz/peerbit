# Integrating wallets

You can use Web3 Wallets with Peerbit. However todo so you need to implement the Peerbit Identity type which requires the wallets publickey and a signing function.

## Ethers.js Wallet

See below for an example of how to integrate the Ethers v6 `Wallet` from the
`ethers` package.

[ethersproject](./ethersproject.ts ":include")
