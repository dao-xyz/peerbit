import { MountDatastore } from "datastore-core";
import { Key } from "interface-datastore/key";
import { LazyLevelDatastore } from "../index.js";
import { interfaceDatastoreTests } from "./interface-datastore-tests.js";

describe("LevelDatastore", () => {
    describe("interface-datastore (leveljs)", () => {
        interfaceDatastoreTests({
            setup: () => new LazyLevelDatastore("hello-" + Math.random()),
        });
    });

    describe("interface-datastore (mount(leveljs, leveljs, leveljs))", () => {
        interfaceDatastoreTests({
            setup() {
                return new MountDatastore([
                    {
                        prefix: new Key("/a"),
                        datastore: new LazyLevelDatastore(
                            "one-" + Math.random()
                        ),
                    },
                    {
                        prefix: new Key("/q"),
                        datastore: new LazyLevelDatastore(
                            "two-" + Math.random()
                        ),
                    },
                    {
                        prefix: new Key("/z"),
                        datastore: new LazyLevelDatastore(
                            "three-" + Math.random()
                        ),
                    },
                ]);
            },
        });
    });
});
