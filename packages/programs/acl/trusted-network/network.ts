import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";

export const getNetwork = (object: any): TrustedNetwork | undefined => {
	return (
		object.constructor.prototype._network &&
		object[object.constructor.prototype._network]
	);
};

export function network(options: { property: string }) {
	return (constructor: any) => {
		constructor.prototype._network = options.property;
	};
}
