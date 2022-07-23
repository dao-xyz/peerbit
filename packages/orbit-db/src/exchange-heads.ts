import Channel from 'ipfs-pubsub-1on1'
import Logger from 'logplease'
import { variant, field, vec, serialize, deserialize } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Message } from './message';
import { HeadsCache, Store } from '@dao-xyz/orbit-db-store';
const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')


@variant(0)
export class ExchangeHeadsMessage extends Message {
  @field({ type: 'String' })
  address: string;

  @field({ type: vec(Entry) })
  heads: Entry[];

  constructor(props?: {
    address: string,
    heads: Entry[]
  }) {
    super();
    if (props) {
      this.address = props.address;
      this.heads = props.heads;
    }
  }

}

const getHeadsForDatabase = async (store: Store<any, any, any>) => {
  if (!(store && store._cache)) return []
  const localHeads = (await store._cache.getBinary(store.localHeadsPath, HeadsCache))?.heads || []
  const remoteHeads = (await store._cache.getBinary(store.remoteHeadsPath, HeadsCache))?.heads || []
  return [...localHeads, ...remoteHeads]
}

export const exchangeHeads = async (ipfs, address, peer, getStore, getDirectConnection, onMessage: (address: string, data: Uint8Array) => void, onChannelCreated) => {
  const _handleMessage = (message: { data: Uint8Array }) => {

    // On message instead,
    onMessage(address, message.data)
    /* const msg = deserialize(Buffer.from(message.data), Message)
    if (msg instanceof ExchangeHeadsMessage) {
      const { address, heads } = msg
      onExchangedHeads(address, heads)
    }
    else {
      throw new Error("Unexpected message")
    } */
  }

  let channel = getDirectConnection(peer)
  if (!channel) {
    try {
      logger.debug(`Create a channel to ${peer}`)
      channel = await Channel.open(ipfs, peer)
      channel.on('message', _handleMessage)
      logger.debug(`Channel created to ${peer}`)
      onChannelCreated(channel)
    } catch (e) {
      logger.error(e)
    }
  }

  // Wait for the direct channel to be fully connected
  await channel.connect()
  logger.debug(`Connected to ${peer}`)

  // Send the heads if we have any
  const heads = await getHeadsForDatabase(getStore(address))
  logger.debug(`Send latest heads of '${address}':\n`, JSON.stringify(heads.map(e => e.hash), null, 2))
  if (heads) {
    const message = serialize(new ExchangeHeadsMessage({ address: address, heads: heads }));
    await channel.send(message)
  }

  return channel
}

