import { Routes } from "../routes";
import crypto from "crypto";

describe("routes", () => {
    /* 
	┌─┐┌─┐
	│a││x│
	└┬┘└┬┘
	┌▽┐┌▽┐
	│b││y│
	└┬┘└─┘
	┌▽┐   
	│c│   
	└─┘   
	*/

    let routes: Routes;
    let a: string, b: string, c: string, x: string, y: string;

    const set = () => {
        a = crypto.randomBytes(16).toString("hex");
        b = crypto.randomBytes(16).toString("hex");
        c = crypto.randomBytes(16).toString("hex");
        x = crypto.randomBytes(16).toString("hex");
        y = crypto.randomBytes(16).toString("hex");
    };
    beforeEach(() => {
        routes = new Routes("_");
        set();
        routes.add(a, b);
        routes.add(b, c);
        routes.add(x, y);
    });
    describe("path", () => {
        it("will find path", () => {
            const path = routes.getPath(a, c);
            expect(path.map((x) => x.id.toString())).toEqual([a, b, c]);
        });

        it("missing node", () => {
            const path = routes.getPath(a, "?");
            expect(path).toHaveLength(0);
        });
        it("missing path", () => {
            const path = routes.getPath(a, x);
            expect(path).toHaveLength(0);
        });
    });

    describe("add", () => {
        it("insertion symmetric", () => {
            const ab = routes.getLink(a, b);
            const ba = routes.getLink(b, a);
            expect(ab).toBeDefined();
            expect(ba).toBeDefined();
        });
    });

    describe("delete", () => {
        it("single", () => {
            routes.deleteLink(b, a);
            expect(routes.getPath(a, c)).toHaveLength(0);
        });

        it("symmetric", () => {
            routes.add(b, a);
            routes.deleteLink(a, b);
            expect(routes.getPath(a, c)).toHaveLength(0);
        });

        it("subgraph 1", () => {
            routes.add(a, x);
            expect(routes.getPath(x, c).length === 4);
            expect(routes.linksCount).toEqual(4);

            routes.deleteLink(a, x, x);
            expect(routes.linksCount).toEqual(1); // x -> y
            expect(routes.getLink(x, y)).toBeDefined();
        });

        it("subgraph 2", () => {
            routes.add(a, x);
            expect(routes.getPath(x, c).length === 4);
            expect(routes.linksCount).toEqual(4);

            routes.deleteLink(a, b, x);
            expect(routes.linksCount).toEqual(2); // x -> y
            expect(routes.getLink(x, a)).toBeDefined();
            expect(routes.getLink(x, y)).toBeDefined();
        });

        it("subgraph 3", () => {
            routes.add(a, x);
            expect(routes.getPath(x, c).length === 4);
            expect(routes.linksCount).toEqual(4);

            routes.deleteLink(a, b, y);
            expect(routes.linksCount).toEqual(2); // x -> a x -> y
            expect(routes.getLink(x, a)).toBeDefined();
            expect(routes.getLink(x, y)).toBeDefined();
        });
    });
});
