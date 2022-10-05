// @ts-ignore
import isNode from 'is-node';

export const max = <T>(...args: T[]) => args.reduce((m, e) => e > m ? e : m);
export const min = <T>(...args: T[]) => args.reduce((m, e) => e < m ? e : m);

export const toBase64 = (arr: Uint8Array) => isNode ? Buffer.from(arr).toString('base64') : window.btoa(String.fromCharCode(...arr));
export const fromBase64 = (base64: string) => isNode ? new Uint8Array(Buffer.from(base64, 'base64')) : Uint8Array.from(atob(base64), c => c.charCodeAt(0))
