import type { Program, ProgramEvents } from "@peerbit/program";
import { PublicSignKey } from "@peerbit/crypto";
import { useCallback, useEffect, useMemo, useState } from "react";
const addressOrDefined = <A, B extends ProgramEvents, P extends Program<A, B>>(
    p?: P
) => {
    try {
        return p?.rootAddress;
    } catch (error) {
        return !!p;
    }
};
type ExtractArgs<T> = T extends Program<infer Args> ? Args : never;
type ExtractEvents<T> = T extends Program<any, infer Events> ? Events : never;

export const useOnline = <
    P extends Program<ExtractArgs<P>, ExtractEvents<P>> &
        Program<any, ProgramEvents>,
>(
    program?: P,
    options?: { id?: string; debug?: boolean }
) => {
    const [peers, setPeers] = useState<PublicSignKey[]>([]);

    const log: (...args: any) => void = useCallback(() => {
        if (!program) {
            return (...args: any[]) => {};
        }
        if (options?.debug) {
            return (...args: any[]) => {
                console.log(
                    `[useOnline ${options?.id ? options.id + " " : ""}${
                        addressOrDefined(program) || "no-address"
                    }]`,
                    ...args
                );
            };
        }
        return () => {};
    }, [program, options?.debug, options?.id]);

    useEffect(() => {
        if (!program || program.closed) {
            log("No program or closed");
            return;
        }
        let changeListener: () => void;

        let closed = false;
        const p = program;
        log("Subscribing to online peers");
        changeListener = () => {
            p.getReady()
                .then((set) => {
                    setPeers([...set.values()]);
                })
                .catch((e) => {
                    console.error(e, closed);
                });
        };
        p.events.addEventListener("join", changeListener);
        p.events.addEventListener("leave", changeListener);
        p.getReady()
            .then((set) => {
                setPeers([...set.values()]);
            })
            .catch((e) => {
                console.log("Error getReady()", {
                    closed,
                    pClosed: p.closed,
                    e,
                });
            });
        // TODO AbortController?

        return () => {
            closed = true;
            p.events.removeEventListener("join", changeListener);
            p.events.removeEventListener("leave", changeListener);
        };
    }, [log, options?.id, addressOrDefined(program)]);
    return {
        peers,
    };
};
