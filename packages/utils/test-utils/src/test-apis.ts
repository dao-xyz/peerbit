/**
 * IPFS daemons to run the tests with.
 */

// Available daemon types are defined in:
// https://github.com/ipfs/js-ipfsd-ctl#ipfsfactory---const-f--ipfsfactorycreateoptions
//

import * as ipfsModule from 'ipfs'
import * as ipfsHttpModule from 'ipfs-http-client'
// @ts-ignore, because go-ipfs does not have typings
import * as ipfsBin from 'go-ipfs'

import dotenv from 'dotenv'
import { NodeType } from 'ipfsd-ctl/src/types'
dotenv.config();

interface Module {
  type: NodeType
  test: boolean,
  disposable: boolean,
  args?: string[]
  ipfsHttpModule?: any,
  ipfsBin?: any,
  ipfsModule?: any
  ipfsOptions?: any // to be set later
}

const jsIpfs = {
  'js-ipfs': {
    type: 'proc' as NodeType,
    test: true,
    disposable: true,
    ipfsModule
  }
}

const goIpfs = {
  'go-ipfs': {
    type: 'go' as NodeType,
    test: true,
    disposable: true,
    args: ['--enable-pubsub-experiment'],
    ipfsHttpModule,
    ipfsBin: ipfsBin.path()
  }
}

// By default, we run tests against js-ipfs.
let testAPIs: { "js-ipfs"?: Module, 'go-ipfs'?: Module, } = Object.assign({}, jsIpfs)

// Setting env variable 'TEST=all' will make tests run with js-ipfs and go-ipfs.
// Setting env variable 'TEST=go' will make tests run with go-ipfs.
// Eg. 'TEST=go mocha' runs tests with go-ipfs
if (process.env.TEST?.toLowerCase() === 'all') {
  testAPIs = Object.assign({}, testAPIs, goIpfs)
} else if (process.env.TEST?.toLowerCase() === 'go') {
  testAPIs = Object.assign({}, goIpfs)
}

export default testAPIs