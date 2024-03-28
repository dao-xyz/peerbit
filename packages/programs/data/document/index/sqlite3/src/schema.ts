import {
	Constructor,
	getSchema,
	FieldType,
	OptionKind,
	VecKind,
	StructKind
} from "@dao-xyz/borsh";
import { SearchRequest } from "@peerbit/document-interface";
import * as types from "@peerbit/document-interface";
import { Field, field as fieldDecalaration, variant } from "@dao-xyz/borsh";
import { logger as loggerFn } from "@peerbit/logger";
import { get } from "http";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { toHexString } from "@peerbit/crypto";
export const logger = loggerFn({ module: "sqlite3-schema" });

const SQLConversionMap = {
	u8: "INTEGER",
	u16: "INTEGER",
	u32: "INTEGER",
	u64: "INTEGER",
	i8: "INTEGER",
	i16: "INTEGER",
	i32: "INTEGER",
	i64: "INTEGER",
	f32: "REAL",
	f64: "REAL",
	bool: "INTEGER",
	string: "TEXT",
	Uint8Array: "BLOB",
	Date: "TEXT"
};
export const coerceSQLType = (
	value: boolean | bigint | string | number | Uint8Array,
	type?: FieldType
): boolean | string | number | Uint8Array => {
	// add bigint when https://github.com/TryGhost/node-sqlite3/pull/1501 fixed

	if (type === "bool") {
		return value == null ? false : (value as boolean);
	}
	if (typeof value === "bigint") {
		return Number(value);
	}
	return value;
};

export const toSQLType = (type: FieldType, isOptional = false) => {
	let ret: string;
	if (typeof type === "string") {
		const sqlType = SQLConversionMap[type];
		if (!sqlType) {
			throw new Error(`Type ${type} is not supported in SQL`);
		}
		ret = sqlType;
	} else if (type === Uint8Array) {
		ret = "BLOB";
	} else if (type instanceof OptionKind) {
		throw new Error("Unexpected option");
	} else if (type instanceof VecKind) {
		throw new Error("Unexpected vec");
	} else {
		throw new Error(`Type ${type} is not supported in SQL`);
	}

	return isOptional ? ret : ret + " NOT NULL";
};

type SQLField = { name: string[]; definition: string; type: string };
type SQLConstraint = { definition: string };
export interface Table {
	name: string;
	ctor: Constructor<any>;
	primary: string;
	fields: SQLField[];
	constraints: SQLConstraint[];
}
export const getSQLTable = (
	ctor: Constructor<any>,
	path: string[],
	primaryKey: string,
	name = getTableName(ctor)
): Table[] => {
	const { constraints, fields, dependencies } = getSQLFields(
		name,
		path,
		ctor,
		primaryKey
	);
	return [
		{ name, constraints, fields, ctor, primary: primaryKey },
		...dependencies
	];
};

export const getTableName = (ctor: Constructor<any>, includePrefix = true) => {
	let name: string;
	const schema = getSchema(ctor);
	if (schema.variant === undefined) {
		logger.warn(
			`Schema associated with ${ctor.name} has no variant.  This will results in SQL table with name generated from the Class name. This is not recommended since changing the class name will result in a new table`
		);
		name = ctor.name;
	} else {
		name =
			typeof schema.variant === "string"
				? schema.variant
				: JSON.stringify(schema.variant);
	}

	// prefix the generated table name so that the name is a valid SQL identifier (table name)
	// choose prefix which is readable and explains that this is a generated table name
	return (includePrefix ? "__" : "") + name.replace(/[^a-zA-Z0-9_]/g, "_");
};

export const getSubTableName = (
	ctor: Constructor<any>,
	key: string[],
	includePrefix = true
) => {
	return `${getTableName(ctor, includePrefix)}__${key.join("_")}`;
};

export const CHILD_TABLE_ID = "__id";
export const PARENT_TABLE_ID = "__parent_id";
const FOREIGN_VALUE_PROPERTY = "value";

export const getSQLFields = (
	tableName: string,
	path: string[],
	ctor: Constructor<any>,
	primaryKey?: string,
	tables: Table[] = [],
	isOptional = false
): {
	fields: SQLField[];
	constraints: SQLConstraint[];
	dependencies: Table[];
} => {
	const schema = getSchema(ctor);
	const fields = schema.fields;
	const sqlFields: SQLField[] = [];
	const sqlConstraints: SQLConstraint[] = [];

	let foundPrimary = false;

	const primaryField = fields.find((x) => x.key === primaryKey);
	/*    if (!primaryField && primaryKey ) { // last argument is to prevent vec(vec) behaviours
		   throw new Error(`Primary key ${primaryKey} not found in schema`)
	   }
	*/
	const handleVecType = (key: string, field: VecKind) => {
		let chilCtor: Constructor<any>;

		if (field.elementType instanceof VecKind) {
			throw new Error("vec(vec(...)) is not supported");
		}

		const subTableName = getSubTableName(ctor, [...path, key]);
		if (
			typeof field.elementType === "function" &&
			field.elementType != Uint8Array
		) {
			chilCtor = field.elementType as Constructor<any>;
		} else {
			class SimpleNested {
				@fieldDecalaration({ type: field.elementType })
				[FOREIGN_VALUE_PROPERTY]: any;

				constructor(value: any) {
					this[FOREIGN_VALUE_PROPERTY] = value;
				}
			}
			chilCtor = SimpleNested;
		}

		const subtables = getSQLTable(chilCtor, path, CHILD_TABLE_ID, subTableName);

		const parentPrimaryField = primaryField || CHILD_TABLE_ID;
		const parentPrimaryFieldType = primaryField
			? toSQLType(primaryField.type)
			: "INTEGER";

		subtables[0].fields = [
			// id
			{
				name: [CHILD_TABLE_ID],
				definition: `${CHILD_TABLE_ID} INTEGER PRIMARY KEY`,
				type: "INTEGER"
			},

			// foreign key parent document
			{
				name: [PARENT_TABLE_ID],
				definition: `${PARENT_TABLE_ID} ${parentPrimaryFieldType}`,
				type: parentPrimaryFieldType
			},

			...subtables[0].fields
		];

		subtables[0].constraints.push({
			definition: `FOREIGN KEY(${PARENT_TABLE_ID}) REFERENCES ${tableName}(${parentPrimaryField})`
		});

		for (const table of subtables) {
			if (!tables.find((x) => x.name === table.name)) {
				tables.push(table);
			}
		}
	};

	const handleSimpleField = (key: string, type: FieldType, isOptional) => {
		const isPrimary = primaryKey === key;
		foundPrimary = foundPrimary || isPrimary;
		const fieldType = toSQLType(type, isOptional);
		sqlFields.push({
			name: [...path, key],
			definition: `'${key}' ${fieldType} ${isPrimary ? "PRIMARY KEY" : ""}`,
			type: fieldType
		});
	};

	for (const field of fields) {
		if (field.type instanceof VecKind) {
			handleVecType(field.key, field.type);
		} else if (field.type instanceof OptionKind) {
			if (field.type.elementType instanceof VecKind) {
				handleVecType(field.key, field.type.elementType);
			} else if (
				typeof field.type.elementType === "string" ||
				field.type.elementType == Uint8Array
			) {
				handleSimpleField(field.key, field.type.elementType, true);
			} else if (field.type.elementType instanceof OptionKind) {
				throw new Error("option(option(T)) not supported");
			} else if (typeof field.type.elementType === "function") {
				const recursive = getSQLFields(
					tableName,
					[...path, field.key],
					field.type.elementType as Constructor<any>,
					primaryKey,
					tables,
					true
				);
				sqlFields.push(...recursive.fields);
				sqlConstraints.push(...recursive.constraints);
			} else {
				throw new Error(
					`Unsupported type in option, ${typeof field.type.elementType}: ${typeof field.type.elementType}`
				);
			}
		} else {
			handleSimpleField(field.key, field.type, isOptional);
		}
	}
	if (!foundPrimary) {
		if (primaryKey != CHILD_TABLE_ID) {
			throw new Error(`Primary key ${primaryKey} not found in schema`);
		} else {
			/*  if (!parent) {
				 throw new Error("Parent is required for nested table")
			 } */
			// create a custom primary key to handle this (foreign table)
			/*   sqlFields = [
  
				  // id
				  { name: [CHILD_TABLE_ID], definition: `${CHILD_TABLE_ID} INTEGER PRIMARY KEY` },
  
				  // foreign key parent document
				  { name: [PARENT_TABLE_ID], definition: `${PARENT_TABLE_ID} ${toSQLType(parent.idField.type)}` },
  
				  ...sqlFields,
  
  
  
			  ] */
			/*  */
			/* sqlConstraints.push({

				definition: `FOREIGN KEY(${PARENT_TABLE_ID}) REFERENCES ${parent.tableName}(${parent.idField.key})`
			}) */
		}
	}
	return {
		fields: sqlFields,
		constraints: sqlConstraints,
		dependencies: tables
	};
};

export const resolveTable = (
	tables: Map<string, Table>,
	ctor: Constructor<any>,
	key?: string[]
) => {
	const name = key == null ? getTableName(ctor) : getSubTableName(ctor, key);
	const table = tables.get(name);
	if (!table) {
		throw new Error(`Table not found for ${name}`);
	}
	return table;
};

export const resolveFieldValues = (
	obj: any,
	path: string[],
	tables: Map<string, Table>,
	table: Table
): { table: Table; values: any[] }[] => {
	const fields = getSchema(table.ctor).fields;
	const result: { table: Table; values: any[] } = { table, values: [] };
	const ret: { table: Table; values: any[] }[] = [];
	const handleVec = (field: Field, optional = false) => {
		if (Array.isArray(obj[field.key])) {
			// TODO this is wrong
			for (const item of obj[field.key]) {
				// let elementType = field.type instanceof OptionKind ? (field.type.elementType as VecKind).elementType : (field.type as VecKind).elementType;
				const subTable = resolveTable(tables, table.ctor, [...path, field.key]);
				const itemResolved = resolveFieldValues(
					new subTable.ctor(item),
					path,
					tables,
					subTable
				);
				itemResolved.forEach((x) => {
					x.values.unshift(obj[table.primary]);
					x.values.unshift(undefined);
				});
				ret.push(...itemResolved);
			}
		} else {
			if (obj[field.key] == null) {
				if (!optional) {
					throw new Error("Expected array, received null");
				} else {
					return;
				}
			}
			throw new Error("Expected array");
		}
	};
	for (const field of fields) {
		if (typeof field.type === "string" || field.type == Uint8Array) {
			result.values.push(coerceSQLType(obj[field.key], field.type));
		} else if (field.type instanceof OptionKind) {
			if (field.type.elementType instanceof VecKind) {
				handleVec(field, true);
			} else {
				result.values.push(
					coerceSQLType(obj[field.key], field.type.elementType)
				);
			}
		} else if (field.type instanceof VecKind) {
			handleVec(field);
		} else {
			// recursive call since the value type is a struct
			ret.push(
				...resolveFieldValues(
					obj[field.key],
					[...path, field.key],
					tables,
					resolveTable(tables, field.type as Constructor<any>)
				)
			);
		}
	}
	return [result, ...ret];
};

export const fromRowToObj = (row: any, ctor: Constructor<any>) => {
	const schema = getSchema(ctor);
	const fields = schema.fields;
	const obj: any = {};
	for (const field of fields) {
		obj[field.key] = row[field.key];
	}
	return Object.assign(Object.create(ctor.prototype), obj);
};

export const convertSearchRequestToQuery = (
	request: SearchRequest,
	tables: Map<string, Table>,
	table: Table
) => {
	let whereBuilder = "";
	let joinBuilder = "";
	let orderByBuilder: string | undefined = undefined;

	if (request.query.length === 1) {
		const { where, join } = convertQueryToSQLQuery(
			request.query[0],
			tables,
			table
		);
		whereBuilder += where;
		if (join) {
			joinBuilder += join;
		}
	} else if (request.query.length > 1) {
		const { where, join } = convertQueryToSQLQuery(
			new types.And(request.query),
			tables,
			table
		);
		whereBuilder += where;
		if (join) {
			joinBuilder += join;
		}
	}

	if (request.sort.length > 0) {
		if (request.sort.length > 0) {
			orderByBuilder = "ORDER BY ";
		}

		orderByBuilder += request.sort
			.map(
				(sort) =>
					`${table.name}.${sort.key} ${sort.direction === types.SortDirection.ASC ? "ASC" : "DESC"}`
			)
			.join(", ");
	}

	return {
		where: whereBuilder.length > 0 ? "where " + whereBuilder : undefined,
		join: joinBuilder.length > 0 ? joinBuilder : undefined,
		orderBy: orderByBuilder
	};
};
export const convertQueryToSQLQuery = (
	query: types.Query,
	tables: Map<string, Table>,
	table: Table
): { where: string; join?: string } => {
	let whereBuilder = "";
	let joinBuilder = "";

	if (query instanceof types.StateFieldQuery) {
		const { where, join } = convertStateFieldQuery(query, tables, table);
		whereBuilder += where;
		join && (joinBuilder += join);
	} else if (query instanceof types.LogicalQuery) {
		if (query instanceof types.And) {
			for (const subquery of query.and) {
				const { where, join } = convertQueryToSQLQuery(subquery, tables, table);
				whereBuilder =
					whereBuilder.length > 0 ? `(${whereBuilder}) AND (${where})` : where;
				join && (joinBuilder += join);
			}
		} else if (query instanceof types.Or) {
			for (const subquery of query.or) {
				const { where, join } = convertQueryToSQLQuery(subquery, tables, table);
				whereBuilder =
					whereBuilder.length > 0 ? `(${whereBuilder}) OR (${where})` : where;
				join && (joinBuilder += join);
			}
		}
	} else {
		throw new Error("Unsupported query type: " + query.constructor.name);
	}

	return {
		where: whereBuilder,
		join: joinBuilder.length > 0 ? joinBuilder : undefined
	};
};

const stringArraysEquals = (a: string[], b: string[]) => {
	if (a.length !== b.length) {
		return false;
	}
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) {
			return false;
		}
	}
	return true;
};

const cloneQuery = (query: types.StateFieldQuery) => {
	return deserialize(serialize(query), types.StateFieldQuery);
};
const convertStateFieldQuery = (
	query: types.StateFieldQuery,
	tables: Map<string, Table>,
	table: Table
): { join?: string; where: string } => {
	// if field id represented as foreign table, do join and compare
	const field = table.fields.find((x) => stringArraysEquals(x.name, query.key));
	const isForeign = field == null;

	if (isForeign) {
		const parentTableIdentifier = `${table.name}.${table.primary}`;
		const joins: string[] = [];
		let foreignTable: Table | undefined = undefined;
		let queryKey = [FOREIGN_VALUE_PROPERTY];
		for (const [i, key] of query.key.entries()) {
			foreignTable = resolveTable(tables, foreignTable?.ctor || table.ctor, [
				key
			]);
			joins.push(
				`JOIN ${foreignTable.name} ON ${parentTableIdentifier} = ${foreignTable.name}.${PARENT_TABLE_ID}`
			);

			if (i === query.key.length - 2) {
				const gotField = !!foreignTable.fields.find((x) =>
					stringArraysEquals(x.name, [query.key[i + 1]])
				);
				if (gotField) {
					queryKey = [query.key[i + 1]];
					break;
				}
			}
		}

		if (!foreignTable) {
			throw new Error("Unexpected");
		}
		query = cloneQuery(query);
		query.key = queryKey;
		const where = `${convertQueryToSQLQuery(query, tables, foreignTable).where}`;
		return { where, join: joins.join(" ") };
		/// return `(${table.name}.${f.key} = ${foreignTable.name}.${CHILD_TABLE_ID}) AND (${convertQueryToSQLQuery(f.query, tables, foreignTable)})`
	}

	const keyWithTable = table.name + "." + query.key.join(".");
	let where: string;
	if (query instanceof types.StringMatch) {
		let statement = "";

		if (query.method === types.StringMatchMethod.contains) {
			statement = `${keyWithTable} LIKE '%${query.value}%'`;
		} else if (query.method === types.StringMatchMethod.prefix) {
			statement = `${keyWithTable} LIKE '${query.value}%'`;
		} else if (query.method === types.StringMatchMethod.exact) {
			statement = `${keyWithTable} = '${query.value}'`;
		}
		if (query.caseInsensitive) {
			statement += " COLLATE NOCASE";
		}
		where = statement;
	} else if (query instanceof types.ByteMatchQuery) {
		// compare Blob compule with f.value

		const statement = `${keyWithTable} = x'${toHexString(query.value)}'`;
		where = statement;
	} else if (query instanceof types.IntegerCompare) {
		if (field.type === "BLOB") {
			// TODO perf
			where = `hex(${keyWithTable}) LIKE '%${toHexString(new Uint8Array([Number(query.value.value)]))}%'`;
		} else if (query.compare === types.Compare.Equal) {
			where = `${keyWithTable} = ${query.value.value}`;
		} else if (query.compare === types.Compare.Greater) {
			where = `${keyWithTable} > ${query.value.value}`;
		} else if (query.compare === types.Compare.Less) {
			where = `${keyWithTable} < ${query.value.value}`;
		} else if (query.compare === types.Compare.GreaterOrEqual) {
			where = `${keyWithTable} >= ${query.value.value}`;
		} else if (query.compare === types.Compare.LessOrEqual) {
			where = `${keyWithTable} <= ${query.value.value}`;
		} else {
			throw new Error(`Unsupported compare type: ${query.compare}`);
		}
	} else if (query instanceof types.MissingField) {
		where = `${keyWithTable} IS NULL`;
	} else if (query instanceof types.BoolQuery) {
		where = `${keyWithTable} = ${query.value}`;
	} else {
		throw new Error("Unsupported query type: " + query.constructor.name);
	}
	return { where };
};
