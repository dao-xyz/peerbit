import { ClosedError, Documents } from "@peerbit/document";
import type { WithContext } from "@peerbit/document";
import { useEffect, useRef, useState } from "react";
import * as indexerTypes from "@peerbit/indexer-interface";
import { debounceLeadingTrailing } from "./utils.js";

type QueryOptons = {
    query: indexerTypes.Query[] | indexerTypes.QueryLike;
    id?: string;
};
export const useCount = <T extends Record<string, any>>(
    db?: Documents<T, any, any>,
    options?: {
        debounce?: number;
        debug?: boolean; // add debug option here
    } & QueryOptons
) => {
    const [count, setCount] = useState<number>(0);
    const countRef = useRef<number>(0);

    useEffect(() => {
        if (!db || db.closed) {
            return;
        }

        const _l = async (args?: any) => {
            try {
                const count = await db.count({
                    query: options?.query,
                    approximate: true,
                });
                countRef.current = count;
                setCount(count);
            } catch (error) {
                if (error instanceof ClosedError) {
                    return;
                }
                throw error;
            }
        };

        const debounced = debounceLeadingTrailing(
            _l,
            options?.debounce ?? 1000
        );

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
        db?.closed ? undefined : db?.rootAddress,
        options?.id ?? options?.query,
    ]);

    return count;
};
