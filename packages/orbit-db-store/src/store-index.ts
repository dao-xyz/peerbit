/*
  Index

  Index contains the state of a datastore, ie. what data we currently have.

  Index receives a call from a Store when the operations log for the Store
  was updated, ie. new operations were added. In updateIndex, the Index
  implements its CRDT logic: add, remove or update items in the data
  structure. Each new operation received from the operations log is applied
  in order onto the current state, ie. each new operation changes the data
  and the state changes.

  Implementing each CRDT as an Index, we can implement both operation-based
  and state-based CRDTs with the same higher level abstractions.

  To read the current state of the database, Index provides a single public
  function: `get()`. It is up to the Store to decide what kind of query
  capabilities it provides to the consumer.

  Usage:
  ```javascript
  const Index = new Index(userId)
  ```
*/

import { Entry, Log } from "@dao-xyz/ipfs-log";

export class Index<T> {


  /*
     @param id - unique identifier of this index, eg. a user id or a hash
   */
  _index?: any;
  id?: any;
  constructor(id?: string) {
    this.id = id
    this._index = []
  }

  /*
   Applies operations to the Index and updates the state
   @param oplog - the source operations log that called updateIndex
   @param entries - operations that were added to the log
 */
  async updateIndex(oplog: Log<Entry<T>>) {
    this._index = oplog.values
  }
}