import path from "path";

export const signingKeysFixturesPath = (dir: string) =>
    path.join(dir, "./fixtures/keys/signing-keys");
export const testKeyStorePath = (dir: string) =>
    path.join("./tmp/keys/signing-keys", dir);
