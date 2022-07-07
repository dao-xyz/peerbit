import { Identities } from "@dao-xyz/orbit-db-identity-provider";
import { Entry } from "./entry";

export interface AccessController<T> {
  canAppend(entry: Entry<T>, identityProvider: Identities): Promise<boolean> | boolean;
}

export class DefaultAccessController<T> implements AccessController<T>{
  async canAppend(entry: Entry<T>, identityProvider: Identities): Promise<boolean> {
    return true
  }
}
