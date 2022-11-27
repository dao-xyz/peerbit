#!/bin/sh 
set -ex 
ipfs bootstrap rm all 
ipfs config Addresses.Swarm '["/ip4/0.0.0.0/tcp/4001", "/ip4/0.0.0.0/tcp/8081/ws", "/ip6/::/tcp/4001"]' --json
ipfs config --json Pubsub.Enabled true 
ipfs config Swarm.RelayService '{"Enabled": true}' --json
