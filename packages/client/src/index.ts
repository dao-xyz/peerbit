import IpfsClient from 'ipfs-http-client'
import OrbitDB from 'orbit-db';

export const createClient = async (url: string | URL | IpfsClient.multiaddr, host = '5001'): Promise<OrbitDB> => {
    const ipfs = IpfsClient.create({
        url,
        host
        // HEADERS for auth
    });
    return await OrbitDB.createInstance(ipfs)
}

export const HELLO = "asd";