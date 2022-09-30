import path from 'path';

/* export const READ_WRITE = {
    read: async (ipfs, cid, options = {}) => {
        const access = await io.read(ipfs, cid, options)
        return (typeof access.write === 'string') ? JSON.parse(access.write) : access.write // v0 access.write not stringified
    },
    write: io.write
} */
export const getEntryKey = (e) => e.v === 0 ? e.key : e.identity.publicKey


// Make sure the given address has '/_access' as the last part
export const ensureAddress = address => {
    const suffix = address.toString().split('/').pop()
    return suffix === '_access'
        ? address
        : path.join(address, '/_access')
}
