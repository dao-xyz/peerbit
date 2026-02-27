export type DirectStreamAckRouteHint = {
	kind: "directstream-ack";
	from: string;
	target: string;
	nextHop: string;
	distance: number;
	session: number;
	updatedAt: number;
	expiresAt?: number;
};

export type FanoutRouteTokenHint = {
	kind: "fanout-token";
	root: string;
	target: string;
	route: string[];
	updatedAt: number;
	expiresAt?: number;
};

export type RouteHint = DirectStreamAckRouteHint | FanoutRouteTokenHint;
