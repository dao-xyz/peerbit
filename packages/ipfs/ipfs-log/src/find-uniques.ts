export const findUniques = <T>(value: T[], key?: string): T[] => {
  // Create an index of the collection
  // TODO fix types. This method is quite ugly, maybe lets remove it altogether
  const uniques: { [key: string | number | symbol]: T } = {}
  const get = (key: string | number | symbol) => uniques[key]
  const addToIndex = (e: T) => (uniques[key ? (e as any)[key] as string | number | symbol : e as string | number | symbol] = e)
  value.forEach(addToIndex)
  return Object.keys(uniques).map(get)
}