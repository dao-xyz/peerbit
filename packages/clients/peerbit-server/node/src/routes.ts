export const getPort = (protocol: string) => {
	if (protocol === "https:") {
		return SSL_PORT;
	}

	if (protocol === "http:") {
		return LOCAL_PORT;
	}

	throw new Error("Unsupported protocol: " + protocol);
};
export const SSL_PORT = 9002;
export const LOCAL_PORT = 8082;
export const TRUST_PATH = "/trust";
export const PEER_ID_PATH = "/peer/id";
export const ADDRESS_PATH = "/peer/address";
export const PROGRAM_PATH = "/program";
export const PROGRAMS_PATH = "/programs";
export const INSTALL_PATH = "/install";
export const BOOTSTRAP_PATH = "/network/bootstrap";
export const RESTART_PATH = "/restart";
export const TERMINATE_PATH = "/terminate";
