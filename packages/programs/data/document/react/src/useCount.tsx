import { ClosedError, type DocumentsLike } from "@peerbit/document";
import * as indexerTypes from "@peerbit/indexer-interface";
import { useEffect, useRef, useState } from "react";
import { debounceLeadingTrailing } from "./utils.js";

type QueryOptons = {
	query: indexerTypes.Query[] | indexerTypes.QueryLike;
	id?: string;
};
export const useCount = <T extends Record<string, any>>(
	db?: DocumentsLike<T, any> & {
		closed?: boolean;
		rootAddress?: string;
		address?: string;
	},
	options?: {
		debounce?: number;
		debug?: boolean; // add debug option here
	} & QueryOptons,
) => {
	const [count, setCount] = useState<number>(0);
	const countRef = useRef<number>(0);
	useEffect(() => {
		if (!db || db.closed) {
			return;
		}

		const _l = async (args?: any) => {
			try {
				const { estimate } = await db.count({
					query: options?.query,
					approximate: true,
				});
				countRef.current = estimate;
				setCount(estimate);
			} catch (error) {
				if (error instanceof ClosedError) {
					return;
				}
				throw error;
			}
		};

		const debounced = debounceLeadingTrailing(_l, options?.debounce ?? 1000);

		const handleChange = () => {
			debounced();
		};

		debounced();
		db.events.addEventListener("change", handleChange);

		return () => {
			db.events.removeEventListener("change", handleChange);
			debounced.cancel();
		};
	}, [
		db?.closed ? undefined : (db?.rootAddress ?? db?.address),
		options?.id ?? options?.query,
	]);

	return count;
};
