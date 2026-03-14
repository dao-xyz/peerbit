export const isFKError = (e: any) => {
	return (
		e?.code === "SQLITE_CONSTRAINT_FOREIGNKEY" ||
		e?.rc === 787 ||
		(e?.message &&
			(e.message.includes("SQLITE_CONSTRAINT_FOREIGNKEY") ||
				e.message.includes("FOREIGN KEY constraint failed")))
	);
};

export const isUniqueConstraintError = (e: any) => {
	return (
		e?.code === "SQLITE_CONSTRAINT_PRIMARYKEY" ||
		e?.code === "SQLITE_CONSTRAINT_UNIQUE" ||
		e?.rc === 1555 ||
		e?.rc === 2067 ||
		(e?.message &&
			(e.message.includes("SQLITE_CONSTRAINT_PRIMARYKEY") ||
				e.message.includes("SQLITE_CONSTRAINT_UNIQUE") ||
				e.message.includes("UNIQUE constraint failed") ||
				e.message.includes("PRIMARY KEY constraint failed")))
	);
};
