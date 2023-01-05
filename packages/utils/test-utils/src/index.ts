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

import waitForPeers from "./wait-for-peers.js";
import { LSession } from "./libp2p.js";
export { createStore } from "./storage.js";

export { LSession, waitForPeers };
