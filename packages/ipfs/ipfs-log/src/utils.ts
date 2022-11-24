export const max = <T>(...args: T[]) => args.reduce((m, e) => (e > m ? e : m));
export const min = <T>(...args: T[]) => args.reduce((m, e) => (e < m ? e : m));
