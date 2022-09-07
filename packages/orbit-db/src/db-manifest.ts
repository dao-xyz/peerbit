import io from '@dao-xyz/orbit-db-io'
import { variant } from '@dao-xyz/borsh';
import { serialize, field } from '@dao-xyz/borsh';
import { Address, Store, Manifest } from '@dao-xyz/orbit-db-store';
/* 
@variant(0)
export class DBManifest<S extends Store<any>>{

  @field({ type: 'string' })
  name: string

  @field({ type: Store })
  store: S


  _address: Address;

  constructor(props?: {
    name: string
    store: S
  }) {
    if (props) {
      this.name = props.name;
      this.store = props.store;
    }
  }

  get address(): Address {
    if (!this._address) {
      throw new Error("Missing address, save once to obtain it")
    }
    return this._address;
  }
  
    async save(ipfs, options: { format?: string, pin?: boolean, timeout?: number } = {}): Promise<Address> {
      const hash = await createDBManifest(ipfs, this, options)
      this._address = Address.parse(Address.join(hash, this.name))
      return this._address
    }
  
    static async load<T, S extends Store<T>>(ipfs, hash: string, options: { timeout?: number } = {}): Promise<DBManifest<S>> {
      const manifest: Manifest = await io.read(ipfs, hash, options);
      const der = deserialize(Buffer.from(manifest.data), DBManifest) as DBManifest<S>
      der._address = Address.parse(Address.join(hash, der.name))
      return der;
    } 
}
// Creates a DB manifest file and saves it in IPFS
const createDBManifest = async (ipfs, db: DBManifest<any>, options?: {
  base?: any;
  pin?: boolean;
  timeout?: number;
  format?: string;
  links?: string[];
}) => {
  
  const manifest: Manifest = {
    data: serialize(db)
  }
  return io.write(ipfs, options.format || 'dag-cbor', manifest, options)
}
 */

/* const manifest = Object.assign({
    name: name,
    type: type,
    accessController: (path.posix || path).join('/ipfs', accessControllerAddress)
  }, */