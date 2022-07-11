export type Hashable = string | { hashCode: () => string }
export const asString = (obj: Hashable) => typeof obj === 'string' ? obj : obj.hashCode()
