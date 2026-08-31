export const normalizeSQLiteDirectory = (
	directory: string | undefined,
): string | undefined => (directory?.length ? directory : undefined);
