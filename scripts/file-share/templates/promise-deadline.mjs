export const withDeadline = async (promise, timeoutMs, message) => {
	if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
		throw new Error("timeoutMs must be a positive safe integer");
	}
	if (typeof message !== "string" || message.length === 0) {
		throw new Error("deadline message must be a non-empty string");
	}

	let timer;
	const deadline = new Promise((_, reject) => {
		timer = setTimeout(() => reject(new Error(message)), timeoutMs);
	});
	try {
		return await Promise.race([promise, deadline]);
	} finally {
		clearTimeout(timer);
	}
};
