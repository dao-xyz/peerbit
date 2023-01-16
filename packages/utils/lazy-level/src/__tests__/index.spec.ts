import { MemoryLevel } from "memory-level";
import { Level } from "level";
import { LazyLevelDatastore } from "../index.js";
import { tempdir } from "./tempdir.js";
import { interfaceDatastoreTests } from "./interface-datastore-tests.js";

describe("LevelDatastore", () => {
    describe("initialization", () => {
        it("should default to a leveldown database", async () => {
            const levelStore = new LazyLevelDatastore("init-default");
            await levelStore.open();

            expect(levelStore.db).toBeInstanceOf(Level);
        });

        it("should be able to override the database", async () => {
            const levelStore = new LazyLevelDatastore(
                new MemoryLevel<string, Uint8Array>({
                    keyEncoding: "utf8",
                    valueEncoding: "view",
                })
            );

            await levelStore.open();

            expect(levelStore.db).toBeInstanceOf(MemoryLevel);
        });
    });

    describe("interface-datastore MemoryLevel", () => {
        interfaceDatastoreTests({
            setup: () =>
                new LazyLevelDatastore(
                    new MemoryLevel<string, Uint8Array>({
                        keyEncoding: "utf8",
                        valueEncoding: "view",
                    })
                ),
        });
    });

    describe("interface-datastore Level", () => {
        interfaceDatastoreTests({
            setup: () => new LazyLevelDatastore(tempdir()),
        });
    });
});
