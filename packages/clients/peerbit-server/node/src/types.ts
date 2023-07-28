export interface StartByVariant {
	variant: string;
}
export interface StartByBase64 {
	base64: string;
}
export type StartProgram = StartByVariant | StartByBase64;
