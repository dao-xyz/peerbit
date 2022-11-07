import { IPFSAddress } from '@dao-xyz/peerbit-crypto';
import { AbstractProgram } from '@dao-xyz/peerbit-program';
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';

export abstract class IVPC {
    _isPermissioned: true
    abstract get network(): TrustedNetwork;
    abstract joinNetwork(): Promise<void>
}

export function VPC<T extends abstract new (...args: any[]) => AbstractProgram>(c: T) {
    abstract class VPC extends c implements IVPC {
        _isPermissioned: true = true
        abstract get network(): TrustedNetwork;
        async joinNetwork() {
            // Will be rejected by peers if my identity is not trusted
            // (this will sign our IPFS ID with our client Ed25519 key identity, if peers do not trust our identity, we will be rejected)
            await this.network.add(new IPFSAddress({ address: (await this.network._ipfs.id()).id.toString() }))
        }
    }

    return VPC
}

