export class AccessError extends Error {
	constructor(message: string = "Access denied") {
		super(message);
	}
}
