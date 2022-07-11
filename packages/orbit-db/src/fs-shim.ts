/* eslint-disable */
const where = require('wherearewe')

export const fs = (!where.isElectronMain && (typeof window === 'object' || typeof self === 'object')) ? null : eval('require("fs")')

