
// get monorepo root location using esm and .git folder
import path from 'path'
import findUp from 'find-up'
const root = path.dirname(findUp.sync('.git', { type: 'directory' }))

export default {
    // global options
    debug: false,
    test: {
        files: [],
        before: () => {
            return { env: { TS_NODE_PROJECT: path.join(root, 'tsconfig.test.json') } }
        }
    }
}
