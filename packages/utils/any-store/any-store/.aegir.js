import path from 'path';
import findUp from 'find-up'
const root = path.dirname(findUp.sync('.git', { type: 'directory' }))

export default {
    test: {
        build: true,
        runner: 'node',
        target: ['node', 'browser', 'webworker'],
        watch: false,
        files: [],
        timeout: 60000,

        grep: '',
        bail: false,
        debug: true,
        progress: false,
        cov: false,
        covTimeout: 60000,
        browser: {
            config: {
                assets: '../../../../node_modules/@peerbit/any-store-opfs/dist',
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