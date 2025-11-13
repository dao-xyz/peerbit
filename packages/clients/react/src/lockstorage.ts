/* 
ISC License (ISC)
Copyright (c) 2016, Wes Cruver <chieffancypants@gmail.com>

Permission to use, copy, modify, and/or distribute this software for any purpose
with or without fee is hereby granted, provided that the above copyright notice
and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
THIS SOFTWARE.
*/

import debugFn from "debug";
import { v4 as uuid } from "uuid";
const debug = debugFn("FastMutex");

export class FastMutex {
    clientId: string;
    xPrefix: string;
    yPrefix: string;
    timeout: number;
    localStorage: any;
    intervals: Map<string, any>;

    constructor({
        clientId = uuid(),
        xPrefix = "_MUTEX_LOCK_X_",
        yPrefix = "_MUTEX_LOCK_Y_",
        timeout = 5000,
        localStorage = undefined as any,
    } = {}) {
        this.clientId = clientId;
        this.xPrefix = xPrefix;
        this.yPrefix = yPrefix;
        this.timeout = timeout;
        this.intervals = new Map();

        this.localStorage = localStorage || window.localStorage;
    }

    lock(
        key: string,
        keepLocked?: () => boolean,
        options?: { replaceIfSameClient?: boolean }
    ): Promise<{
        restartCount: number;
        contentionCount: number;
        locksLost: number;
    }> {
        debug(
            'Attempting to acquire Lock on "%s" using FastMutex instance "%s"',
            key,
            this.clientId
        );
        const x = this.xPrefix + key;
        const y = this.yPrefix + key;
        let acquireStart = Date.now();
        return new Promise((resolve, reject) => {
            let restartCount = 0;
            let contentionCount = 0;
            let locksLost = 0;

            const acquireLock = (key: string) => {
                // If the option is set and the same client already holds both keys,
                // update the expiry and resolve immediately.
                if (options?.replaceIfSameClient) {
                    const currentX = this.getItem(x);
                    const currentY = this.getItem(y);
                    if (
                        currentX === this.clientId &&
                        currentY === this.clientId
                    ) {
                        // Update expiry so that the lock is effectively "replaced"
                        this.setItem(x, this.clientId, keepLocked);
                        this.setItem(y, this.clientId, keepLocked);
                        debug(
                            'FastMutex client "%s" replaced its own lock on "%s".',
                            this.clientId,
                            key
                        );
                        return resolve({
                            restartCount,
                            contentionCount,
                            locksLost,
                        });
                    }
                }

                // Check for overall retries/timeouts.
                if (
                    restartCount > 1000 ||
                    contentionCount > 1000 ||
                    locksLost > 1000
                ) {
                    return reject("Failed to resolve lock");
                }
                const elapsedTime = Date.now() - acquireStart;
                if (elapsedTime >= this.timeout) {
                    debug(
                        'Lock on "%s" could not be acquired within %sms by FastMutex client "%s"',
                        key,
                        this.timeout,
                        this.clientId
                    );
                    return reject(
                        new Error(
                            `Lock could not be acquired within ${this.timeout}ms`
                        )
                    );
                }

                // First, set key X.
                this.setItem(x, this.clientId, keepLocked);

                // Check if key Y exists (another client may be acquiring the lock)
                let lsY = this.getItem(y);
                if (lsY) {
                    debug("Lock exists on Y (%s), restarting...", lsY);
                    restartCount++;
                    setTimeout(() => acquireLock(key), 10);
                    return;
                }

                // Request the inner lock by setting Y.
                this.setItem(y, this.clientId, keepLocked);

                // Re-check X; if it was changed, we have contention.
                let lsX = this.getItem(x);
                if (lsX !== this.clientId) {
                    contentionCount++;
                    debug('Lock contention detected. X="%s"', lsX);
                    setTimeout(() => {
                        lsY = this.getItem(y);
                        if (lsY === this.clientId) {
                            debug(
                                'FastMutex client "%s" won the lock contention on "%s"',
                                this.clientId,
                                key
                            );
                            resolve({
                                restartCount,
                                contentionCount,
                                locksLost,
                            });
                        } else {
                            restartCount++;
                            locksLost++;
                            debug(
                                'FastMutex client "%s" lost the lock contention on "%s" to another process (%s). Restarting...',
                                this.clientId,
                                key,
                                lsY
                            );
                            setTimeout(() => acquireLock(key), 10);
                        }
                    }, 50);
                    return;
                }

                // No contention: lock is acquired.
                debug(
                    'FastMutex client "%s" acquired a lock on "%s" with no contention',
                    this.clientId,
                    key
                );
                resolve({ restartCount, contentionCount, locksLost });
            };

            acquireLock(key);
        });
    }

    isLocked(key: string) {
        const x = this.xPrefix + key;
        const y = this.yPrefix + key;
        return !!this.getItem(x) || !!this.getItem(y);
    }

    getLockedInfo(key: string): string | undefined {
        const x = this.xPrefix + key;
        const y = this.yPrefix + key;
        return this.getItem(x) || this.getItem(y);
    }

    release(key: string) {
        debug(
            'FastMutex client "%s" is releasing lock on "%s"',
            this.clientId,
            key
        );
        let ps = [this.yPrefix + key, this.xPrefix + key];
        for (const p of ps) {
            clearInterval(this.intervals.get(p));
            this.intervals.delete(p);
            this.localStorage.removeItem(p);
        }
    }

    /**
     * Helper function to wrap all values in an object that includes the time (so
     * that we can expire it in the future) and json.stringify's it
     */
    setItem(key: string, value: any, keepLocked?: () => boolean) {
        if (!keepLocked) {
            return this.localStorage.setItem(
                key,
                JSON.stringify({
                    expiresAt: new Date().getTime() + this.timeout,
                    value,
                })
            );
        } else {
            let getExpiry = () => +new Date() + this.timeout;
            const ret = this.localStorage.setItem(
                key,
                JSON.stringify({
                    expiresAt: getExpiry(),
                    value,
                })
            );
            const interval = setInterval(() => {
                if (!keepLocked()) {
                    this.localStorage.setItem(
                        // TODO, release directly?
                        key,
                        JSON.stringify({
                            expiresAt: 0,
                            value,
                        })
                    );
                } else {
                    this.localStorage.setItem(
                        key,
                        JSON.stringify({
                            expiresAt: getExpiry(), // bump expiry
                            value,
                        })
                    );
                }
            }, this.timeout);
            this.intervals.set(key, interval);
            return ret;
        }
    }

    /**
     * Helper function to parse JSON encoded values set in localStorage
     */
    getItem(key: string): string | undefined {
        const item = this.localStorage.getItem(key);
        if (!item) return;

        const parsed = JSON.parse(item);
        if (new Date().getTime() - parsed.expiresAt >= this.timeout) {
            debug(
                'FastMutex client "%s" removed an expired record on "%s"',
                this.clientId,
                key
            );
            this.localStorage.removeItem(key);
            clearInterval(this.intervals.get(key));
            this.intervals.delete(key);
            return;
        }

        return JSON.parse(item).value;
    }
}
