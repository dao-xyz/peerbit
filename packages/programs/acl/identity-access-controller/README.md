# "Chain agnostic" Access Controller

An access controller that supports different layers of controll and fallbacks. 

- A store containing ACL information, for example what public key can read and write
- A distributed relation store that lets you use linked devices to get access
- A fallback trusted network access controller that lets you have access if you are trusted by the root trust identity


As of know, go through the test for documentation