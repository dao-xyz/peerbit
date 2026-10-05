import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519PublicKey } from "@peerbit/crypto";
import {
	type DocumentsLike,
	type SearchRequest,
	StringMatch,
} from "@peerbit/document";
import { expect } from "chai";
import {
	type FromTo,
	IdentityRelation,
	TrustedNetwork,
	getPathGenerator,
	getToByFrom,
} from "../src/index.js";

const names = ["R", "A", "B", "C", "D", "E", "X", "Y", "Z"] as const;
type Name = (typeof names)[number];
type Edge = [Name, Name];
const keys = Object.fromEntries(
	names.map((name, i) => [
		name,
		new Ed25519PublicKey({ publicKey: new Uint8Array(32).fill(i + 1) }),
	]),
) as Record<Name, Ed25519PublicKey>;
const hashes = (values: Name[]) => values.map((name) => keys[name].hashcode());
const diamond: Edge[] = [
	["R", "A"],
	["R", "B"],
	["A", "C"],
	["B", "C"],
	["C", "D"],
	["D", "E"],
];
const cycle: Edge[] = [
	["R", "A"],
	["R", "B"],
	["A", "R"],
	["A", "C"],
	["B", "D"],
	["C", "A"],
	["D", "E"],
];

const fixture = (edges: Edge[], decode: boolean, root: Name = "R") => {
	const relations = edges.map(
		([from, to]) => new IdentityRelation({ from: keys[from], to: keys[to] }),
	);
	const queries: { value: string; options: unknown }[] = [];
	const network = new TrustedNetwork({ rootTrust: keys[root] });
	network.trustGraph = {
		index: {
			search: async (request: SearchRequest, options: unknown) => {
				// Ignore the best-effort remote warmup after a denied local check.
				if (request.query.length === 0) {
					return [];
				}
				const query = request.query[0];
				expect(query).to.be.instanceOf(StringMatch);
				const match = query as StringMatch;
				queries.push({ value: match.value, options });
				// Bound regressions that would otherwise loop forever on fresh objects.
				if (queries.length > names.length) {
					throw new Error("Trust walk repeatedly expanded the same identities");
				}
				expect(match.key).to.have.length(1);
				expect(match.key[0]).to.be.oneOf(["from", "to"]);
				return relations
					.filter(
						(relation) =>
							relation[match.key[0] as "from" | "to"].hashcode() ===
							match.value,
					)
					.map((relation) =>
						decode
							? deserialize(serialize(relation), IdentityRelation)
							: relation,
					);
			},
		},
	} as unknown as DocumentsLike<IdentityRelation, FromTo>;
	return { network, relations, queries };
};

describe("trust walk", () => {
	for (const decode of [false, true]) {
		describe(
			decode ? "freshly decoded relations" : "shared relation objects",
			() => {
				it("enumerates unique identities beyond a diamond", async () => {
					const { network, queries } = fixture(diamond, decode);
					expect(
						(await network.getTrusted()).map((key) => key.hashcode()),
					).to.deep.equal(hashes(["R", "A", "B", "C", "D", "E"]));
					expect(queries.map(({ value }) => value)).to.deep.equal(
						hashes(["R", "A", "B", "C", "D", "E"]),
					);
				});

				it("finds the trust root beyond a reverse diamond using local queries", async () => {
					const { network, queries } = fixture(
						diamond.map(([from, to]) => [to, from]),
						decode,
						"E",
					);
					expect(await network.isTrusted(keys.R)).to.be.true;
					expect(queries.map(({ value }) => value)).to.deep.equal(
						hashes(["R", "A", "B", "C", "D"]),
					);
					for (const { options } of queries) {
						expect(options).to.deep.equal({ remote: false, local: true });
					}
				});

				it("enumerates each identity once through cycles including the root", async () => {
					const { network, queries } = fixture(cycle, decode);
					expect(
						(await network.getTrusted()).map((key) => key.hashcode()),
					).to.deep.equal(hashes(["R", "A", "B", "C", "D", "E"]));
					expect(queries.map(({ value }) => value)).to.deep.equal(
						hashes(["R", "A", "B", "C", "D", "E"]),
					);
				});

				it("yields every reachable edge while expanding each identity once", async () => {
					const { network, relations, queries } = fixture(cycle, decode);
					const ids: Uint8Array[] = [];
					for await (const relation of getPathGenerator(
						keys.R,
						network.trustGraph,
						getToByFrom,
					)) {
						ids.push(relation.id);
					}
					expect(ids).to.deep.equal(relations.map(({ id }) => id));
					expect(queries.map(({ value }) => value)).to.deep.equal(
						hashes(["R", "A", "B", "C", "D", "E"]),
					);
				});

				it("denies unknown identities and cycles disconnected from the root", async () => {
					for (const name of ["X", "Z"] as const) {
						const { network, queries } = fixture(
							[...cycle, ["X", "Y"], ["Y", "X"]],
							decode,
						);
						expect(await network.isTrusted(keys[name])).to.be.false;
						expect(queries.map(({ value }) => value)).to.deep.equal(
							hashes(name === "X" ? ["X", "Y"] : ["Z"]),
						);
						for (const { options } of queries) {
							expect(options).to.deep.equal({ remote: false, local: true });
						}
					}
				});
			},
		);
	}
});
