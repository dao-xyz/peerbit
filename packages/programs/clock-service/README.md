# ClockService

This module can sign entries to verify that their timestamp are set correctly 

See the [test](./src/__tests__/index.test.ts) for examples

Nodes running this program will sign entries if their timestamp is not too far from "now". 
Hence, only use this program with an identity dedicated for this job since peers can send any entry to be signed (i.e. use dedicated nodes for this)