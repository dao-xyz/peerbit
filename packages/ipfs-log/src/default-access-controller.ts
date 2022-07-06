import Identities from "orbit-db-identity-provider";
import { Entry } from "./signable";

export interface AccessController {
  canAppend(entry: Entry, identityProvider: Identities): Promise<boolean> | boolean;
}

export class DefaultAccessController implements AccessController {
  async canAppend(entry: Entry, identityProvider: Identities): Promise<boolean> {
    return true
  }
}
