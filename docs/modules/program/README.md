# Programs
You can think of the program component as a database definition with all associated logic: Who can append? Who can read? What happens when an entry gets added? 


In the *future* program will more or less behave like smart contracts, with its own runtime, for now they are simple JavaScript classes that extends ```Program``` class. 


[definition](./example.ts ':include :fragment=definition')


A program lets you write control mechanism for Append-Only logs (which are represented as a [Log](./packages/log), example program



