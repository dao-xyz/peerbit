import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';

export interface Networked {
    networkType: string;
}

export interface VPC extends Networked {
    networkType: 'VPC'

    get network(): TrustedNetwork;
}
export const isVPC = (object: any): object is VPC => {
    return object.networkType === 'VPC'
}