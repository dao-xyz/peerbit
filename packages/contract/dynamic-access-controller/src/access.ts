import { field, option, variant, vec } from "@dao-xyz/borsh"
import { serialize } from "@dao-xyz/borsh"
import { AccessCondition } from "./condition"
import { SystemBinaryPayload } from '@dao-xyz/bpayload';

export enum AccessType {
    Any = 0,
    Read = 1,
    Write = 2
}



@variant(4)
export class AccessData extends SystemBinaryPayload {

}

@variant(0)
export class Access extends AccessData {

    @field({ type: option('string') })
    id: string

    @field({ type: vec('u8') })
    accessTypes: AccessType[]

    @field({ type: AccessCondition })
    accessCondition: AccessCondition<any>

    constructor(options?: { accessTypes: AccessType[], accessCondition: AccessCondition<any> }) {
        super();
        if (options) {
            this.accessTypes = options.accessTypes;
            this.accessCondition = options.accessCondition;
            this.initialize();
        }
    }


    calculateId(): string {
        if (!this.accessTypes || !this.accessCondition) {
            throw new Error("Not initialized");
        }
        const a = new Access();
        a.accessCondition = this.accessCondition;
        a.accessTypes = this.accessTypes;
        return Buffer.from(serialize(a)).toString('base64')
    }

    initialize(): Access {
        this.id = this.calculateId();
        return this;
    }

    assertId() {
        const calculatedId = this.calculateId();
        if (this.id !== calculatedId) {
            throw new Error(`Invalid id, got ${this.id} but expected ${calculatedId}`)
        }
    }
}
