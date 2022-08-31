export const bigIntMax = (...args) => args.reduce((m, e) => e > m ? e : m);
export const bigIntMin = (...args) => args.reduce((m, e) => e < m ? e : m);
