# Ensuring Unique Addresses and Data Integrity

Peerbit employs a distinctive approach to guarantee unique addresses for databases, maintain data integrity, and enforce access control.

## Unique Database Addresses

Peerbit generates unique program addresses based on the program specification or what, in many cases, would represent a database schema. These addresses are calculated using a combination of schema properties, ensuring that each database is uniquely identifiable.

Users also have the option to manually specify when composing higher-order functionality with building blocks like the [Document store](/modules/program/document-store/). This manual identification ensures that databases with identical schemas remain distinguishable. See [here](/modules/program/document-store/?id=determinism) for example how you can preconfigure identifiers for a document store so that the resulting db address will be deterministic.

## Schema-Based Access Control

Peerbit empowers users to define access control rules during database schema creation. The level of access control a user has depends on the modules used. These rules determine who can perform specific operations within the database, preventing unauthorized access and data manipulation.

Read more [here](/modules/program/document-store/?id=definition) about the access control that can be set on a document store.

## Data Integrity Measures

By creating unique database addresses and employing access control, Peerbit minimizes the risk of data collisions and conflicts. Each database operates independently, safeguarding its integrity.

## Pubsub Topics for Database Communication

Peerbit relies on pubsub topics for communication about databases and program functionality in general. The topics are dynamic and depend on the identifiers and attributes set by users when creating their program specification. This ensures that all communications are focused and context-specific. In essence, databases built with Peerbit employ an innovative approach that revolves around the generation of unique addresses, schema-based access control, and data integrity measures. This ensures that each database remains distinct, secure, and free from conflicts.