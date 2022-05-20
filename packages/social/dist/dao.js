"use strict";
/**
 * A decentralized storage of DAO meta data info.
 * The nodes governing this storage are part of a "official" set of nodes
 * These nodes are trusted by the dao.xyz.dao to act truthfully to serve
 * the wider community a way of creating, modifying, deleting and searching
 * DAOs (organizations/communities/groups)
 */
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDAOs = exports.DaoDB = exports.DAO = void 0;
const borsh_1 = require("@dao-xyz/borsh");
const node_1 = require("@dao-xyz/node");
let DAO = class DAO {
    name;
};
DAO = __decorate([
    (0, borsh_1.variant)(0)
], DAO);
exports.DAO = DAO;
class DaoDB extends node_1.ShardedDB {
    db;
    daos;
    constructor() {
        super();
    }
    async create(options) {
        await super.create({
            ...options, ...{
                behaviours: {
                    typeMap: {
                        [DAO.name]: DAO
                    }
                },
                repo: './ipfs',
                replicationCapacity: 512 * 1000,
            }
        });
        //  --- Create
        let rootChains = this.shardChainChain;
        // Create Root shard
        await rootChains.addPeerToShards();
        // Create/Load DAO store
        let daoStoreOptions = new node_1.BinaryDocumentStoreOptions({
            indexBy: "name",
            objectType: DAO.name
        });
        this.daos = await this.loadShardChain("dao", daoStoreOptions);
    }
    async support() {
        await this.daos.addPeerToShards({
            peersLimit: 1,
            startIndex: 0,
            supportAmountOfShards: 1
        });
    }
}
exports.DaoDB = DaoDB;
const getDAOs = () => { };
exports.getDAOs = getDAOs;
//# sourceMappingURL=dao.js.map