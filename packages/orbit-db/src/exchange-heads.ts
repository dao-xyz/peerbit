import { variant, field, vec, serialize } from '@dao-xyz/borsh';
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Message } from './message';
import { HeadsCache, Store } from '@dao-xyz/orbit-db-store';
import Logger from 'logplease'
import { DecryptedThing, MaybeSigned } from '@dao-xyz/encryption-utils';
import { Ed25519PublicKey } from 'sodium-plus';
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



const getHeadsForDatabase = async (store: Store<any>) => {
  if (!(store && store._cache)) return []
  const localHeads = (await store._cache.getBinary(store.localHeadsPath, HeadsCache))?.heads || []
  const remoteHeads = (await store._cache.getBinary(store.remoteHeadsPath, HeadsCache))?.heads || []
  return [...localHeads, ...remoteHeads]
}

export const exchangeHeads = async (channel: any, topic: string, getStore: (address: string) => { [key: string]: Store<any> }, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: Ed25519PublicKey }>) => {

  // Send the heads if we have any
  const stores = getStore(topic);
  for (const [storeAddress, store] of Object.entries(stores)) {
    const heads = await getHeadsForDatabase(store)
    logger.debug(`Send latest heads of '${topic}':\n`, JSON.stringify(heads.map(e => e.hash), null, 2))
    if (heads) {
      const message = new ExchangeHeadsMessage({ replicationTopic: topic, address: storeAddress, heads: heads });
      const signedMessage = await new MaybeSigned({ data: serialize(message) }).sign(sign)
      const decryptedMessage = new DecryptedThing({
        data: serialize(signedMessage)
      })
      const serializedMessage = serialize(decryptedMessage);
      await channel.send(serializedMessage)
    }
  }
}

