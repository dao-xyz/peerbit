import type {
	Address,
	OpenOptions,
	Program,
	ProgramClient,
} from "@peerbit/program";
import type { CanonicalClient } from "./client.js";

export type CanonicalOpenMode = "auto" | "local" | "canonical";

export type CanonicalOpenResult<T> = {
	proxy: T;
	address?: Address;
};

export type CanonicalOpenAdapter<
	S extends Program<any> = Program<any>,
	T = unknown,
> = {
	name: string;
	canOpen(program: Program<any>): program is S;
	getKey?(program: S, options?: OpenOptions<S>): string | undefined;
	open(ctx: {
		program: S;
		options: OpenOptions<S>;
		peer: ProgramClient;
		client: CanonicalClient;
	}): Promise<CanonicalOpenResult<T>>;
};

type ManagedProxy = {
	parents: any[];
	children: any[];
	closed: boolean;
	close: (from?: any) => Promise<boolean>;
	drop: (from?: any) => Promise<boolean>;
	emitEvent?: (event: CustomEvent) => void;
	address?: Address;
	rootAddress?: Address;
	isRoot?: boolean;
	node?: ProgramClient;
	save?: () => Promise<Address>;
	delete?: () => Promise<void>;
};

export const createManagedProxy = <T extends { close: () => Promise<void> }>(
	proxy: T,
	options: {
		address?: Address;
		node?: ProgramClient;
		onClose?: () => void;
	} = {},
): T & ManagedProxy => {
	const parents: any[] = [];
	const children: any[] = [];
	let closed = false;
	const rawClose = proxy.close.bind(proxy);

	const close = async (from?: any): Promise<boolean> => {
		if (closed) return true;
		if (from != null) {
			const idx = parents.findIndex((p) => p === from);
			if (idx === -1) {
				throw new Error("Could not find parent");
			}
			if (parents.length > 1) {
				parents.splice(idx, 1);
				return false;
			}
		}

		const pending = children.splice(0);
		await Promise.all(
			pending.map((child) =>
				typeof child?.close === "function"
					? child.close(proxy as any)
					: undefined,
			),
		);

		await rawClose();
		closed = true;
		(proxy as any).closed = true;
		options.onClose?.();
		return true;
	};

	const drop = async (from?: any): Promise<boolean> => {
		return close(from);
	};

	const emitEvent = (event: CustomEvent) => {
		const events = (proxy as any).events;
		if (events && typeof events.dispatchEvent === "function") {
			events.dispatchEvent(event);
		}
	};

	const isRoot = () => {
		if (!parents || parents.length === 0) return true;
		return parents.filter((parent) => !!parent).length === 0;
	};

	const rootAddress = () => {
		let root: any = proxy;
		while (root?.parents && root.parents.length > 0) {
			if (root.parents.length > 1) {
				throw new Error("Multiple parents not supported");
			}
			const parent = root.parents[0];
			if (!parent) break;
			root = parent;
		}
		const address = root?.address;
		if (!address) {
			throw new Error("Proxy has no address");
		}
		return address as Address;
	};

	const save = async () => {
		if (options.address) return options.address;
		if ((proxy as any).address) return (proxy as any).address as Address;
		throw new Error("Proxy has no address");
	};

	const del = async () => {
		await close();
	};

	Object.assign(proxy as any, {
		parents,
		children,
		close,
		drop,
		emitEvent,
	});
	(proxy as any).closed = closed;
	if (!("isRoot" in (proxy as any))) {
		Object.defineProperty(proxy as any, "isRoot", {
			get: isRoot,
			enumerable: true,
		});
	}
	if (!("rootAddress" in (proxy as any))) {
		Object.defineProperty(proxy as any, "rootAddress", {
			get: rootAddress,
			enumerable: true,
		});
	}

	if (options.address) {
		(proxy as any).address = options.address;
	}
	if (options.node) {
		(proxy as any).node = options.node;
	}
	if (typeof (proxy as any).save !== "function") {
		(proxy as any).save = save;
	}
	if (typeof (proxy as any).delete !== "function") {
		(proxy as any).delete = del;
	}

	return proxy as T & ManagedProxy;
};
