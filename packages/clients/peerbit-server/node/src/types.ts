export interface StartByVariant {
	variant: string;
}
export interface StartByBase64 {
	base64: string;
}
export interface AnyArgs {
	[key: string]: any; // Allow extra generic properties
}
export type StartProgram = (StartByVariant | StartByBase64) & AnyArgs;

export interface InstallWithTGZ {
	type: "tgz";
	name: string;
	base64: string;
}

export interface InstallWithNPM {
	type: "npm";
	name: string;
}

export type InstallDependency = InstallWithTGZ | InstallWithNPM;
