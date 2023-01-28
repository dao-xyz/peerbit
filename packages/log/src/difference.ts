export const difference = (a: any, b: any, key: string) => {
	// Indices for quick lookups
	const processed: { [key: string]: any } = {};
	const existing: { [key: string]: any } = {};

	// Create an index of the first collection
	const addToIndex = (e: any) => (existing[key ? e[key] : e] = true);
	a.forEach(addToIndex);

	// Reduce to entries that are not in the first collection
	const reducer = (res: any, entry: any) => {
		const isInFirst = existing[key ? entry[key] : entry] !== undefined;
		const hasBeenProcessed = processed[key ? entry[key] : entry] !== undefined;
		if (!isInFirst && !hasBeenProcessed) {
			res.push(entry);
			processed[key ? entry[key] : entry] = true;
		}
		return res;
	};

	return b.reduce(reducer, []);
};
