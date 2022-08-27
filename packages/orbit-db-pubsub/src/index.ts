
import pSeries from 'p-series'
import PeerMonitor from 'ipfs-pubsub-peer-monitor'
import Logger from 'logplease';
import { v4 as uuid } from 'uuid';
const logger = Logger.create("pubsub", { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')

const maxTopicsOpen = 256
let topicsOpenCount = 0

export class Message {
  id: string;
  from: string;
  topicIDs?: string[];
  topic?: string;
  data: Buffer;
}

export type Subscription = {
  id: string,
  topicMonitor: PeerMonitor,
  onNewPeer: (topic: string, peer: string, subscriptionId: string) => void,
  onMessage: (topicId: string, content: Uint8Array, from: string) => void,
  dependencies: Set<string>
};
export class PubSub {
  _ipfs: any;
  _id: any;
  _subscriptions: {
    [key: string]: Subscription
  };

  constructor(ipfs, id: string) {
    this._ipfs = ipfs
    this._id = id
    this._subscriptions = {}

    if (this._ipfs.pubsub === null)
      logger.error("The provided version of ipfs doesn't have pubsub support. Messages will not be exchanged.")

    this._handleMessage = this._handleMessage.bind(this)

    // Bump up the number of listeners we can have open,
    // ie. number of databases replicating
    if (this._ipfs.setMaxListeners)
      this._ipfs.setMaxListeners(maxTopicsOpen)
  }

  async subscribe(topic: string, subscriberId: string, onMessageCallback: (topic: string, content: any, from: any) => void, onNewPeerCallback: (topic: string, peer: string, subscriptionId: string) => void, options = {}): Promise<string> {
    let existingSubscription = this._subscriptions[topic];
    if (!existingSubscription && this._ipfs.pubsub) {
      await this._ipfs.pubsub.subscribe(topic, this._handleMessage, options)

      const topicMonitor = new PeerMonitor(this._ipfs.pubsub, topic)

      topicMonitor.on('join', (peer) => {
        logger.debug(`Peer joined ${topic}:`)
        logger.debug(peer)
        const subscription = this._subscriptions[topic];
        if (subscription) {
          onNewPeerCallback(topic, peer, subscription.id)
        } else {
          logger.warn('Peer joined a room we don\'t have a subscription for')
          logger.warn(topic, peer)
        }
      })

      topicMonitor.on('leave', (peer) => logger.debug(`Peer ${peer} left ${topic}`))
      topicMonitor.on('error', (e) => logger.error(e))
      const id = uuid();
      this._subscriptions[topic] = {
        id,
        topicMonitor: topicMonitor,
        onMessage: onMessageCallback,
        onNewPeer: onNewPeerCallback,
        dependencies: new Set([subscriberId])
      }
      topicsOpenCount++
      logger.debug("Topics open:", topicsOpenCount)
      return id;
    }
    else {
      existingSubscription.dependencies.add(subscriberId);
      return existingSubscription.id;
    }

  }

  async unsubscribe(hash: string, subscriberId: string, ignoreDependencies = false): Promise<string> {
    const subscription = this._subscriptions[hash];
    if (subscription) {
      subscription.dependencies.delete(subscriberId);
      if (subscription.dependencies.size === 0 || ignoreDependencies) {
        await this._ipfs.pubsub.unsubscribe(hash, this._handleMessage)
        this._subscriptions[hash].topicMonitor.stop()
        delete this._subscriptions[hash]
        logger.debug(`Unsubscribed from '${hash}'`)
        topicsOpenCount--
        logger.debug("Topics open:", topicsOpenCount)
      }
      return subscription.id
    }
    return undefined
  }

  publish(topic: string, payload: Uint8Array, options = {}) {
    if (this._subscriptions[topic] && this._ipfs.pubsub) {
      /*       let payload;
       */      //Buffer should be already serialized. Everything else will get serialized as json if not buffer, string.
      /*  if (Buffer.isBuffer(message) || typeof message === "string") {
         payload = message;
       } else {
         payload = JSON.stringify(message);
       } */
      this._ipfs.pubsub.publish(topic, Buffer.from(payload), options)
    }
  }

  async disconnect() {
    const topics = Object.keys(this._subscriptions)
    await pSeries(topics.map((t) => this.unsubscribe.bind(this, t)))
    this._subscriptions = {}
  }

  async _handleMessage(message: Message) {
    // Don't process our own messages
    if (message.from === this._id)
      return

    // Get the message content and a subscription
    let content, topicId, subscription

    // Get the topic. Compat with ipfs js 62 and 63
    topicId = message.topic ? message.topic : message.topicIDs[0]

    content = message.data;
    subscription = this._subscriptions[topicId]

    if (subscription && subscription.onMessage && content) {
      await subscription.onMessage(topicId, content, message.from)
    }
  }
}