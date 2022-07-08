import { field, variant, vec } from "@dao-xyz/borsh"
import { serialize } from "@dao-xyz/borsh"
import { AccessCondition } from "./condition"
import bs58 from 'bs58';
export enum AccessType {
    Admin = 0,
    /* Add = 1,
    Remove = 2,
    ModifySelf = 3, */
}

@variant(0)
export class AccessData {

}

@variant(0)
export class Access extends AccessData {

    @field({ type: 'String' })
    id: string

    @field({ type: vec('u8') })
    accessTypes: AccessType[]

    @field({ type: AccessCondition })
    accessCondition: AccessCondition

    constructor(options?: { accessTypes: AccessType[], accessCondition: AccessCondition }) {
        super();
        if (options) {
            Object.assign(this, options);
            this.setId();
        }
    }


    calculateId(): string {
        if (!this.accessTypes || !this.accessCondition) {
            throw new Error("Not initialized");
        }
        return bs58.encode(Buffer.from(serialize(this)))
    }
    setId(): void {
        this.id = undefined;
        this.id = this.calculateId();
    }

    assertId() {
        const calculatedId = this.calculateId();
        if (this.id !== calculatedId) {
            throw new Error(`Invalid id, got ${this.id} but expected ${calculatedId}`)
        }
    }
}
