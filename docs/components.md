
## [Programs](./packages/programs)
Contains composable programs you can build your program with. For example distributed [document store](./packages/programs/data/document), [clock service](./packages/programs/clock-service), [chain agnostic access controller](./packages/programs/acl/identity-access-controller) 

A program lets you write control mechanism for Append-Only logs (which are represented as a [Log](./packages/log), example program

```typescript 
import { Peerbit } from '@dao-xyz/peerbit'
import { Log } from '@dao-xyz/peerbit-log'
import { Program } from '@dao-xyz/peerbit-program' 
import { field, variant } from '@dao-xyz/borst-ts' 

@variant("string_store") // Needs to have a variant name so the program is unique
class StringStore extends Program  // Needs to extend Program if you are going to store Store<any> in your class
{
    @field({type: Log}) // decorate it for serialization purposes 
    log: Log<string>

    constructor(properties: { log?: Log<string>}) {
        this.log = properties.log || new Log()
    }

    async setup() 
    {
        // some setup routine that is called before the Program opens
        await log.setup({ encoding: ... , canAppend: ..., canRead: ...})
    }
}



// Later 

const peer = await Peerbit.create()

const program = await peer.open(new StringStore(), ... options ...)
 
console.log(program.address) // "peerbit/123xyz..." 

// Now you can interact the log through 
program.log.append( ... )
```

See the [DString](./packages/programs/data/string) for a complete working example that also includes a string search index
