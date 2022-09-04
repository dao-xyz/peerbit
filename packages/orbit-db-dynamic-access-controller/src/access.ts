import { field, option, variant, vec } from "@dao-xyz/borsh"
import { serialize } from "@dao-xyz/borsh"
import { AccessCondition } from "./condition"
import { BinaryPayload } from '@dao-xyz/bpayload';
export enum AccessType {
    Admin = 0,
    Read = 1,
    Write = 2
    /* Add = 1,
    Remove = 2,
    ModifySelf = 3, */
}



@variant("access")
export class AccessData extends BinaryPayload {

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
            Object.assign(this, options);
        }
    }


    calculateId(): string {
        if (!this.accessTypes || !this.accessCondition) {
            throw new Error("Not initialized");
        }
        return Buffer.from(serialize(new Access({
            accessCondition: this.accessCondition,
            accessTypes: this.accessTypes
        }))).toString('base64')
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
