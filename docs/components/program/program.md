# Program
You can think of the program component as a database definition with all associated logic: Who can append? Who can read? What happens when an entry gets added? 
In the future program will more or less behave like smart contracts, for now they are simple classes extending the ```Program``` class. 

[definition](./example.ts ':include :fragment=definition')
Contains composable programs you can build your program with. For example distributed [document store](./packages/programs/data/document), [clock service](./packages/programs/clock-service), [chain agnostic access controller](./packages/programs/acl/identity-access-controller) 

A program lets you write control mechanism for Append-Only logs (which are represented as a [Log](./packages/log), example program