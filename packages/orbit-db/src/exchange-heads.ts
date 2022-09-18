import { variant, field, vec, serialize } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Message } from './message';
import { StoreLike } from '@dao-xyz/orbit-db-store';
import Logger from 'logplease'
import { DecryptedThing } from '@dao-xyz/encryption-utils';
import { MaybeSigned, PublicKey } from '@dao-xyz/identity';
const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends Message {

  @field({ type: 'string' })
  replicationTopic: string;

  @field({ type: 'string' })
  address: string;

  @field({ type: vec(Entry) })
  heads: Entry<T>[];

  constructor(props?: {
    replicationTopic: string,
    address: string,
    heads: Entry<T>[]
  }) {
    super();
    if (props) {
      this.replicationTopic = props.replicationTopic;
      this.address = props.address;
      this.heads = props.heads;
    }
  }
}

@variant([0, 1])
export class RequestHeadsMessage extends Message {

  @field({ type: 'string' })
  replicationTopic: string;

  @field({ type: 'string' })
  address: string;

  constructor(props?: {
    replicationTopic: string,
    address: string
  }) {
    super();
    if (props) {
      this.replicationTopic = props.replicationTopic;
      this.address = props.address;
    }
  }
}




export const exchangeHeads = async (channel: any, topic: string, getStore: (address: string) => { [key: string]: StoreLike<any> }, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>) => {

  // Send the heads if we have any
  const stores = getStore(topic);
  if (stores) {
    for (const [storeAddress, store] of Object.entries(stores)) {
      const heads = await store.getHeads();
      logger.debug(`Send latest heads of '${topic}':\n`, JSON.stringify(heads.map(e => e.hash), null, 2))
      if (heads && heads.length > 0) {
        const message = new ExchangeHeadsMessage({ replicationTopic: topic, address: storeAddress, heads: heads });
        const signedMessage = await new MaybeSigned({ data: serialize(message) }).sign(sign)
        const decryptedMessage = new DecryptedThing({
          data: serialize(signedMessage)
        }) // TODO encryption?
        const serializedMessage = serialize(decryptedMessage);
        await channel.send(serializedMessage)
      }
    }
  }

}

