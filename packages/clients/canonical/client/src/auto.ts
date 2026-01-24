import { getSchema } from "@dao-xyz/borsh";
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

/**
 * Canonical open supports "proxy parents" (managed proxies returned by adapters),
 * which are not instances of `Program`. This widens `OpenOptions.parent` so apps
 * don't need `parent: proxy as any`.
 */
export type CanonicalOpenOptions<S extends Program<any>> = Omit<
	OpenOptions<S>,
	"parent"
> & {
	parent?: unknown;
};

export type CanonicalOpenAdapter<
	S extends Program<any> = Program<any>,
	T = unknown,
> = {
	name: string;
	/**
	 * Optional list of borsh @variant strings this adapter can open.
	 * If `canOpen` is omitted, canonical open will match by comparing
	 * `getSchema(program.constructor).variant` against these values.
	 */
	variant?: string;
	variants?: string[];
	canOpen?(program: Program<any>): program is S;
	getKey?(program: S, options?: OpenOptions<S>): string | undefined;
	open(ctx: {
		program: S;
		options: OpenOptions<S>;
		peer: ProgramClient;
		client: CanonicalClient;
	}): Promise<CanonicalOpenResult<T>>;
};

export const getProgramVariant = (
	program: Program<any>,
): string | undefined => {
	if (!program || typeof program !== "object") return undefined;
	try {
		const schema = getSchema((program as any).constructor);
		const variant = schema?.variant;
		return typeof variant === "string" ? variant : undefined;
	} catch {
		return undefined;
	}
};

export const createVariantAdapter = <
	S extends Program<any> = Program<any>,
	T = unknown,
>(options: {
	name: string;
	variant: string | string[];
	getKey?: (program: S, options?: OpenOptions<S>) => string | undefined;
	open: CanonicalOpenAdapter<S, T>["open"];
}): CanonicalOpenAdapter<S, T> => {
	const variants = (
		Array.isArray(options.variant) ? options.variant : [options.variant]
	).map(String);
	return {
		name: options.name,
		variants,
		canOpen: (program: Program<any>): program is S => {
			const candidate = getProgramVariant(program);
			return !!candidate && variants.includes(candidate);
		},
		getKey: options.getKey,
		open: options.open,
	};
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
