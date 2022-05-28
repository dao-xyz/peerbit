export type ToStringable = string | { toString: () => string }
export const asString = (obj: ToStringable) => typeof obj === 'string' ? obj : obj.toString()
