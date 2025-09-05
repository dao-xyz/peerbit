export const getPort = (protocol: string) => {
	if (protocol === "https:") {
		return REMOTE_API_PORT;
	}

	if (protocol === "http:") {
		return LOCAL_API_PORT;
	}

	throw new Error("Unsupported protocol: " + protocol);
};
export const REMOTE_API_PORT = 9002;
export const LOCAL_API_PORT = 8082;
export const TRUST_PATH = "/trust";
export const PEER_ID_PATH = "/peer/id";
export const ADDRESS_PATH = "/peer/address";
export const PROGRAM_PATH = "/program";
export const PROGRAMS_PATH = "/programs";
export const PROGRAM_VARIANTS_PATH = "/program/variants";
export const INSTALL_PATH = "/install";
export const BOOTSTRAP_PATH = "/network/bootstrap";
export const RESTART_PATH = "/restart";
export const TERMINATE_PATH = "/terminate";
export const STOP_PATH = "/stop";
export const LOG_PATH = "/log";
export const VERSIONS_PATH = "/versions";
export const SELF_UPDATE_PATH = "/self/update";
