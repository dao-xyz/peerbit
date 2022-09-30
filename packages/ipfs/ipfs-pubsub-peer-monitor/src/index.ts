import EventEmitter from "events"
import { difference } from "./utils";


const DEFAULT_OPTIONS = {
  start: true,
  pollInterval: 1000,
}

export class IpfsPubsubPeerMonitor extends EventEmitter {
  _pubsub: any;
  _topic: string;
  _options: any;
  _peers: string[];
  _interval: any;

  constructor(ipfsPubsub: any, topic: string, options?: { start?: boolean, pollInterval?: number }) {
    super()
    this._pubsub = ipfsPubsub
    this._topic = topic
    this._options = Object.assign({}, DEFAULT_OPTIONS, options)
    this._peers = []
    this._interval = null

    if (this._options.start)
      this.start()
  }

  get topic() {
    return this._topic;
  }
  get started() { return this._interval !== null }
  set started(val) { throw new Error("'started' is read-only") }

  start() {
    if (this._interval)
      this.stop()

    this._interval = setInterval(
      this._pollPeers.bind(this),
      this._options.pollInterval
    )
    this._pollPeers()
  }

  stop() {
    clearInterval(this._interval)
    this._interval = null
    this.removeAllListeners('error')
    this.removeAllListeners('join')
    this.removeAllListeners('leave')
  }

  async getPeers() {
    this._peers = await this._pubsub.peers(this._topic)
    return this._peers.slice()
  }

  hasPeer(peer) {
    return this._peers.includes(peer)
  }

  async _pollPeers() {
    try {
      const peers = await this._pubsub.peers(this._topic)
      IpfsPubsubPeerMonitor._emitJoinsAndLeaves(new Set(this._peers), new Set(peers), this)
      this._peers = peers
    } catch (err) {
      clearInterval(this._interval)
      this.emit('error', err)
    }
  }

  static _emitJoinsAndLeaves(oldValues, newValues, events) {
    const emitJoin = addedPeer => events.emit('join', addedPeer)
    const emitLeave = removedPeer => events.emit('leave', removedPeer)
    difference(newValues, oldValues).forEach(emitJoin)
    difference(oldValues, newValues).forEach(emitLeave)
  }
}

