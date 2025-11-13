import { ClosedError, Documents } from "@peerbit/document";
import type { WithContext } from "@peerbit/document";
import { useEffect, useRef, useState } from "react";
import * as indexerTypes from "@peerbit/indexer-interface";
import { debounceLeadingTrailing } from "./utils.js";

type QueryLike = {
    query?: indexerTypes.Query[] | indexerTypes.QueryLike;
    sort?: indexerTypes.Sort[] | indexerTypes.Sort | indexerTypes.SortLike;
};
type QueryOptons = {
    query: QueryLike;
    id: string;
};

const logWithId = (
    options: { debug?: boolean | { id: string } } | undefined,
    ...args: any[]
) => {
    if (!options?.debug) return;

    if (typeof options.debug === "boolean") {
        console.log(...args);
    } else if (typeof options.debug.id === "string") {
        console.log(options.debug.id, ...args);
    }
};

export const useLocal = <
    T extends Record<string, any>,
    I extends Record<string, any>,
    R extends boolean | undefined = true,
    RT = R extends false ? WithContext<I> : WithContext<T>,
>(
    db?: Documents<T, I>,
    options?: {
        resolve?: R;
        transform?: (result: RT) => Promise<RT>;
        onChanges?: (all: RT[]) => void;
        debounce?: number;
        debug?: boolean | { id: string };
    } & QueryOptons
) => {
    const [all, setAll] = useState<RT[]>([]);
    const emptyResultsRef = useRef(false);

    useEffect(() => {
        if (!db || db.closed || options?.query === null) {
            // null query means no query at all
            return;
        }

        logWithId(
            options,
            "reset local",
            db?.closed ? undefined : db?.rootAddress,
            options?.id,
            options?.resolve,
            options?.onChanges,
            options?.transform
        );

        const _l = async (args?: any) => {
            try {
                const iterator = db.index.iterate(options?.query ?? {}, {
                    local: true,
                    remote: false,
                    resolve: options?.resolve as any,
                });

                let results: RT[] = (await iterator.all()) as any;

                if (options?.transform) {
                    results = await Promise.all(
                        results.map((x) => options.transform!(x))
                    );
                }
                logWithId(
                    options,
                    options?.id,
                    results,
                    "query",
                    options?.query
                );

                emptyResultsRef.current = results.length === 0;
                setAll(() => {
                    options?.onChanges?.(results);
                    return results;
                });
            } catch (error) {
                if (error instanceof ClosedError) {
                    return;
                }
                throw error;
            }
        };

        const debounced = debounceLeadingTrailing(_l, options?.debounce ?? 123);

        const handleChange = () => {
            if (emptyResultsRef.current) {
                debounced.cancel();
                _l();
            } else {
                debounced();
            }
        };

        debounced();
        db.events.addEventListener("change", handleChange);

        return () => {
            db.events.removeEventListener("change", handleChange);
            debounced.cancel();
        };
    }, [
        db?.closed ? undefined : db?.rootAddress,
        options?.id,
        options?.resolve,
        options?.onChanges,
        options?.transform,
    ]);

    return all;
};
