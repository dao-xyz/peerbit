import EventEmitter from 'events'
import { getPeerID } from './get-peer-id';
import { waitForPeers } from './wait-for-peers';
import { v1 as PROTOCOL } from './protocol';

export type PubSubMessage = {
  data: Buffer
  from: string
  key: Buffer
  receivedFrom: string
  seqno: Buffer
  signature: Uint8Array
  topicIDs: string[]
};

/**
 * Communication channel over Pubsub between two IPFS nodes
 */
export class DirectChannel extends EventEmitter {
  _id: string;
  _ipfs: any;
  _closed: boolean;
  _isClosed: () => void;
  _receiverID: string;
  _senderID: string;
  _peers: string[];
  _messageHandler: (msg: { from: string, data: Buffer }) => void;

  constructor(ipfs, receiverID: string) {
    super()

    // IPFS instance to use internally
    this._ipfs = ipfs

    if (!ipfs.pubsub) {
      throw new Error('This IPFS node does not support pubsub.')
    }

    this._closed = false
    this._isClosed = () => this._closed
    this._receiverID = receiverID

    if (!this._receiverID) {
      throw new Error('Receiver ID was undefined')
    }
    // See _setup() for more state initialization
  }

  /**
   * Channel ID
   * @return {[String]} Channel's ID
   */
  get id() {
    return this._id
  }

  get recieverId() {
    return this._receiverID;
  }

  get senderId() {
    return this._senderID;
  }


  /**
   * Peers participating in this channel
   * @return {[Array]} Array of peer IDs participating in this channel
   */
  get peers() {
    return this._peers
  }

  async connect() {
    await waitForPeers(this._ipfs, [this._receiverID], this._id, this._isClosed)
  }

  /**
   * Send a message to the other peer
   * @param  {[Any]} message Payload
   */
  async send(message: string | Buffer) {
    if (this._closed) return
    await this._ipfs.pubsub.publish(this._id, Buffer.isBuffer(message) ? message : Buffer.from(message))
  }

  /**
   * Close the channel
   */
  async close(): Promise<void> {
    this._closed = true
    this.removeAllListeners('message')
    return this._ipfs.pubsub.unsubscribe(this._id, this._messageHandler)
  }

  async _setup() {
    this._senderID = await getPeerID(this._ipfs)

    // Channel's participants
    this._peers = Array.from([this._senderID, this._receiverID]).sort()

    // ID of the channel is "<peer1 id>/<peer 2 id>""
    this._id = '/' + PROTOCOL + '/' + this._peers.join('/')

    // Function to use to handle incoming messages
    this._messageHandler = (message: {
      data: Buffer
      from: string
      key: Buffer
      receivedFrom: string
      seqno: Buffer
      signature: Uint8Array
      topicIDs: string[]
    }) => {

      // Make sure the message is coming from the correct peer
      const isValid = message && message.from === this._receiverID

      // Filter out all messages that didn't come from the second peer

      if (isValid) {
        this.emit('message', message)
      }
    }
  }

  async _openChannel() {
    this._closed = false
    await this._setup()
    await this._ipfs.pubsub.subscribe(this._id, this._messageHandler)
  }

  static async open(ipfs, receiverID: string) {
    const channel = new DirectChannel(ipfs, receiverID)
    await channel._openChannel()
    return channel
  }
}

