import { Connection } from '@solana/web3.js';
import fs from 'mz/fs';
import os from 'os';
import path from 'path';
import yaml from 'yaml';
/**
 * @private
 */
async function getConfig(): Promise<any> {
    // Path to Solana CLI config file
    const CONFIG_FILE_PATH = path.resolve(os.homedir(), '.config', 'solana', 'cli', 'config.yml');
    const configYml = await fs.readFile(CONFIG_FILE_PATH, { encoding: 'utf8' });
    return yaml.parse(configYml);
}

/**
 * Load and parse the Solana CLI config file to determine which RPC url to use
 */
export async function getRpcUrl(): Promise<string> {
    try {
        const config = await getConfig();
        if (!config.json_rpc_url) throw new Error('Missing RPC URL');
        return config.json_rpc_url;
    } catch (err) {
        console.warn('Failed to read RPC url from CLI config file, falling back to localhost');
        return 'http://localhost:8899';
    }
}

export const establishConnection = async (rpcUrl?: string): Promise<Connection> => {
    if (!rpcUrl) rpcUrl = await getRpcUrl();
    const connection = new Connection(rpcUrl, 'confirmed');
    return connection;
};
