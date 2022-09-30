// Set utils
export const difference = (set1, set2) => new Set([...set1].filter(x => !set2.has(x)))

// Poll utils
const sleep = (time) => new Promise(resolve => setTimeout(resolve, time))

export const runWithDelay = async (func, topic, interval) => {
  const peers = await func(topic)
  await sleep(interval)
  return peers
}

