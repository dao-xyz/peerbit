import fs from 'fs';
import { Level } from 'level';
import { MemoryLevel } from 'memory-level';

const prefixPath = './keystore-test/'
export const createStore = (name = 'keystore'): Level => {
    if (fs && fs.mkdirSync) {
        fs.mkdirSync(prefixPath + name, { recursive: true })
    }
    return new MemoryLevel({ valueEncoding: 'view' }) as Level
}