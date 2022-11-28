/* import EventEmitter from 'events'

const originalAddListener = EventEmitter.prototype.addListener;


EventEmitter.prototype.addListener = function (type, ...args) {

  console.log('here');

  const numListeners = (this as any).listeners(type).length;
  const max = typeof (this as any)._maxListeners === 'number' ? (this as any)._maxListeners : 10;

  if (max !== 0 && numListeners > max) {
    const error = new Error('Too many listeners of type "' + type.toString() + '" added to EventEmitter. Max is ' + max + " and we've added " + numListeners + '.');
    console.error(error);
    throw error;
  }
  return originalAddListener.apply(this, [type, ...args])
}; */

import connectPeers from "./connect-peers.js";
export * from "./session.js";
import getIpfsPeerId from "./get-ipfs-peer-id.js";
import stopIpfs from "./stop-ipfs.js";
import testAPIs from "./test-apis.js";
import waitForPeers from "./wait-for-peers.js";
import { browserConfig, factoryConfig, nodeConfig } from "./config.js";
import { startIpfs } from "./start-ipfs.js";
import { MemStore } from "./mem-store.js";
export { createStore } from "./storage.js";
import { LSession } from "./libp2p.js";

export {
    MemStore,
    LSession,
    connectPeers,
    getIpfsPeerId,
    startIpfs,
    stopIpfs,
    testAPIs,
    waitForPeers,
    browserConfig,
    factoryConfig,
    nodeConfig,
};
