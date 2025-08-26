export const isFKError = (e: any) => {
	return (
		e?.code === "SQLITE_CONSTRAINT_FOREIGNKEY" ||
		e?.rc === 787 ||
		(e?.message &&
			(e.message.includes("SQLITE_CONSTRAINT_FOREIGNKEY") ||
				e.message.includes("FOREIGN KEY constraint failed")))
	);
};
