import Channel from 'ipfs-pubsub-1on1'
import Logger from 'logplease'
import { variant, field, vec, serialize, deserialize } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Message } from './message';
const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')


@variant(0)
export class ExchangeHeadsMessage {
  @field({ type: 'String' })
  address: string;

  @field({ type: vec(Entry) })
  heads: Entry[];

  constructor(props?: {
    address: string,
    heads: Entry[]
  }) {
    if (props) {
      this.address = props.address;
      this.heads = props.heads;
    }
  }

}

const getHeadsForDatabase = async store => {
  if (!(store && store._cache)) return []
  const localHeads = await store._cache.get(store.localHeadsPath) || []
  const remoteHeads = await store._cache.get(store.remoteHeadsPath) || []
  return [...localHeads, ...remoteHeads]
}

export const exchangeHeads = async (ipfs, address, peer, getStore, getDirectConnection, onMessage, onChannelCreated) => {
  const _handleMessage = (message: { data: Uint8Array }) => {
    const msg = deserialize(Buffer.from(message.data), Message)
    if (msg instanceof ExchangeHeadsMessage) {
      const { address, heads } = msg
      onMessage(address, heads)
    }
    else {
      throw new Error("Unexpected message")
    }
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
    await channel.send(serialize(new ExchangeHeadsMessage({ address: address, heads: heads })))
  }

  return channel
}

