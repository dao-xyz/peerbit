import type { PeerId } from "@libp2p/interface-peer-id";

const DEFAULT_OPTIONS = {
  start: true,
  pollInterval: 1000,
};
interface PubSub {
  peers: (topic: string) => Promise<(PeerId | string)[]>;
}
const difference = (set1: string[], set2: string[]): string[] => {
  // assume size of set1 and set2 are small
  return set1.filter((p) => !set2.find((p2) => p2 === p));
};

interface Callbacks {
  onJoin?: (peer: string) => void;
  onLeave?: (peer: string) => void;
  onError?: (err: any) => void;
}
export class IpfsPubsubPeerMonitor {
  _pubsub: PubSub;
  _topic: string;
  _options: any;
  _peers: string[];
  _interval: any;
  _callbacks: Callbacks;
  constructor(
    pubsub: PubSub,
    topic: string,
    callbacks: Callbacks,
    options?: { start?: boolean; pollInterval?: number }
  ) {
    this._pubsub = pubsub;
    this._topic = topic;
    this._options = Object.assign({}, DEFAULT_OPTIONS, options);
    this._peers = [];
    this._interval = null;
    this._callbacks = callbacks;

    if (this._options.start) this.start();
  }

  get topic() {
    return this._topic;
  }
  get started() {
    return this._interval !== null;
  }
  set started(val) {
    throw new Error("'started' is read-only");
  }

  start() {
    if (this._interval) this.stop();

    this._interval = setInterval(
      this._pollPeers.bind(this),
      this._options.pollInterval
    );
    this._pollPeers();
  }

  stop() {
    clearInterval(this._interval);
    this._interval = null;
  }

  async getPeers() {
    this._peers = (await this._pubsub.peers(this._topic)).map((x) =>
      x.toString()
    );
    return this._peers.slice();
  }

  hasPeer(peer: string) {
    return this._peers.map((p) => p === peer);
  }

  async _pollPeers() {
    try {
      const peers = (await this._pubsub.peers(this._topic)).map((x) =>
        x.toString()
      );
      IpfsPubsubPeerMonitor._emitJoinsAndLeaves(
        this._peers,
        peers,
        this._callbacks
      );
      this._peers = peers;
    } catch (err) {
      clearInterval(this._interval);
      this._callbacks.onError && this._callbacks.onError(err);
    }
  }

  static _emitJoinsAndLeaves(
    oldValues: string[],
    newValues: string[],
    callbacks: Callbacks
  ) {
    const emitJoin = (addedPeer: string) =>
      callbacks.onJoin && callbacks.onJoin(addedPeer);
    const emitLeave = (removedPeer: string) =>
      callbacks.onLeave && callbacks.onLeave(removedPeer);
    difference(newValues, oldValues).forEach(emitJoin);
    difference(oldValues, newValues).forEach(emitLeave);
  }
}
