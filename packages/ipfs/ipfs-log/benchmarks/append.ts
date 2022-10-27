const { startIpfs, stopIpfs, config } = require('@dao-xyz/peerbit-test-utils')
import { createLog } from './utils/create-log'

const base = {
  prepare: async function () {
    const ipfsd = await startIpfs('js-ipfs', config)
    const { log } = await createLog(ipfsd.api, 'A')
    return { log, ipfsd }
  },
  cycle: async function ({ log }) {
    await log.append('Hello', 32)
  },
  teardown: async function ({ ipfsd }) {
    await stopIpfs(ipfsd)
  }
}

const baseline = {
  while: ({ stats, startTime, baselineLimit }) => {
    return stats.count < baselineLimit
  }
}

const stress = {
  while: ({ stats, startTime, stressLimit }) => {
    return process.hrtime(startTime)[0] < stressLimit
  }
}

export default [
  { id: 'append-baseline', ...base, ...baseline },
  { id: 'append-stress', ...base, ...stress }
]
