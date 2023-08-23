# "Chain agnostic" Access Controller
## 🚧 Experimental state 🚧

An access controller that supports different layers of control and fallbacks. 

- A store containing ACL information, for example what public key can read and write
- A distributed relation store that lets you use linked devices to get access
- A fallback trusted network access controller that lets you have access if you are trusted by the root trust identity


As of now, go through the tests for documentation.
