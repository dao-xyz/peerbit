
/* 
import path from 'path'
import findUp from 'find-up'
const root = path.dirname(findUp.sync('.git', { type: 'directory' }))
export default {
    test: {
        browser: {
            config: {
                 assets: '../../../../../../node_modules/@sqlite.org/sqlite-wasm/sqlite-wasm/jswasm', 
                  headers: {
                     'Cross-Origin-Opener-Policy': 'same-origin',
                     'Cross-Origin-Embedder-Policy': 'require-corp',
                 },
                buildConfig: {
                    conditions: ['production']
                },
                debug: true,
            },
            debug: true,
        },
        before: () => {
            return { env: { TS_NODE_PROJECT: path.join(root, 'tsconfig.test.json') } }
        }
    },

} */

import path, { dirname } from 'path'
import findUp from 'find-up'

const root = path.dirname(findUp.sync('.git', { type: 'directory' }))

export default {

    // test cmd options
    build: {
        bundle: true,
        bundlesize: false,
        bundlesizeMax: '100kB',
        types: true,
        config: {
            format: 'esm',
            minify: false,
            outfile: "dist/peerbit/sqlite3.min.js",
            banner: { js: '' },
            footer: { js: '' }
        }
    },
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
                assets: ['../../../../node_modules/@sqlite.org/sqlite-wasm/sqlite-wasm/jswasm', './dist'],
                /* path.join(dirname(import.meta.url), "../", './xyz') ,*//* 
                headers: {
                    'Cross-Origin-Opener-Policy': 'same-origin',
                    'Cross-Origin-Embedder-Policy': 'require-corp',
                }, */
                buildConfig: {
                    conditions: ['production']
                },
            },
        },
        before: () => {
            return { env: { TS_NODE_PROJECT: path.join(root, 'tsconfig.test.json') } }
        }
    }
}
