export const getConfigDir = async (): Promise<string> => {
    const path = await import("path");
    const os = await import("os");
    const configDir = path.join(os.homedir(), ".peerbit");
    return configDir;
};

export const getCredentialsPath = async (
    configDir: string
): Promise<string> => {
    const path = await import("path");
    return path.join(configDir, "credentials");
};

export const getKeysPath = async (configDir: string): Promise<string> => {
    const path = await import("path");
    return path.join(configDir, "keys");
};

export class NotFoundError extends Error {}
