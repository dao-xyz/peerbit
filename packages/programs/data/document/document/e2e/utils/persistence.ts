import type { Page } from "@playwright/test";

export type PeerInfo = { peerHash?: string; persisted?: boolean };

export async function waitForPeerInfo(page: Page, timeout = 15000) {
    await page.waitForFunction(() => !!(window as any).__peerInfo?.peerHash, {
        timeout,
    });
}

export async function getPeerInfo(page: Page): Promise<PeerInfo> {
    return (await page.evaluate(() => (window as any).__peerInfo)) || {};
}

export async function expectPersistent(page: Page) {
    await waitForPeerInfo(page);
    const info = await getPeerInfo(page);
    if (!info.persisted) {
        throw new Error(
            `Expected persisted=true, got ${JSON.stringify(info, null, 2)}`
        );
    }
    if (!info.peerHash) {
        throw new Error("peerHash not available");
    }
    return info;
}
