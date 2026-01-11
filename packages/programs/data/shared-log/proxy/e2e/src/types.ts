export const makeTestId = (seed: string, marker: number): Uint8Array => {
	const id = new Uint8Array(32);
	id[0] = marker;
	const bytes = new TextEncoder().encode(seed);
	for (let i = 0; i < bytes.length; i++) {
		id[1 + (i % 31)] ^= bytes[i]!;
	}
	return id;
};
