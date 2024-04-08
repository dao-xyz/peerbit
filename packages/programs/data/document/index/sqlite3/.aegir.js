import path from 'path'
import findUp from 'find-up'
const root = path.dirname(findUp.sync('.git', { type: 'directory' }))


export default {
    test: {
        browser: {
            config: {
                assets: '../../../../../../node_modules/@sqlite.org/sqlite-wasm/sqlite-wasm/jswasm',
                buildConfig: {
                    conditions: ['production']
                }
            }
        },
        before: () => {
            return { env: { TS_NODE_PROJECT: path.join(root, 'tsconfig.test.json') } }
        }
    },

}