# Composition
Programs can be building blocks for others. The [Document store](./packages/programs/data/document) is a program that is tailored for storing and retrieving documents. 

You can include any external program into your program by adding it to you class definition. Below is an example how a forum database could be defined using composition. 


[composition](./composition.ts ':include')

Here are a few exmaples of programs that you can compose your program with: [Document store](./packages/programs/data/document), [Clock service](./packages/programs/clock-service), [Chain agnostic access controller](./packages/programs/acl/identity-access-controller) 