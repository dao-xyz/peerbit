# Document store
The document store is program/database you can use within your apps. The purpose of the document store is to act as a key-value storage for any kind of values. Additionally it also support document query, such as field search and sorting.

The document store is a separate package and you can install it with 

```
npm install @peerbit/document
```


## Imports
[imports](./document-store.ts ':include :fragment=imports')

## Definition
Below is an example of a definition of two document stores where we store posts (and comments), and their reactions. 

[imports](./document-store.ts ':include :fragment=definition')

## Put
Inserting documents are done like below

[put](./document-store.ts ':include :fragment=insert')


## Delete
You can also delete documents

[delete](./document-store.ts ':include :fragment=delete')


## Searching
Now from another client lets connect and try to find our documents

### Get all

[get-all](./document-store.ts ':include :fragment=search-all')



### Local only
We can also search for only local documents

[insert](./document-store.ts ':include :fragment=search-locally')

### Query

We can add conditions for our query.

For example, finding all post created by someone

[search-from-one](./document-store.ts ':include :fragment=search-from-one')

And for our reactions, we can find them for a particular post

[search-reactions-from-one](./document-store.ts ':include :fragment=reactions-one')


#### Query types
There are many different kinds of filter you can apply

[query-types](./document-store.ts ':include :fragment=query-detailed')


## Sorting 

You can apply one or more sort criteria. This will be evaluated in order, if there is a tie-break for the first criteria, the next will be evaluated 


[sort-detailed](./document-store.ts ':include :fragment=sort-detailed')


## Iterator

The iterator is a recommend way of collecting and iterating over documents as this provides the finest amount of control on how and when to fetch more documents from peers. The difference between search and iterate, is that the search api creates an iterator under the hood and performs ```iterator.next()``` and tries to collect as many documents as possible, while an iterator allow you to call next yourself.

[iterator-detailed](./document-store.ts ':include :fragment=iterator-detailed')

## Syncing 
You can sync documents to you local store while iterating or searching. 

[sync](./document-store.ts ':include :fragment=sync')


## Replication degree 

Below is an additional example how you granulary can control
- Replication degree as opening argument.
- Replication degree on put. 
- Filtering on commits based on allowed replication degree 
- What peers to trust as replicators

[replication-degree](./replication-degree.ts ':include')
