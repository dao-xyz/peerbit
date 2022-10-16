import fs from 'fs';
import { Level } from 'level';
const prefixPath = 'packages/store/dstore/src/__tests__/keystore-test/'
export const createStore = (name = 'keystore'): Level => {
    if (fs && fs.mkdirSync) {
        fs.mkdirSync(prefixPath + name, { recursive: true })
    }
    return new Level(prefixPath + name, { valueEncoding: 'view' })
}