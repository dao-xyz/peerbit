import {
	type Constructor,
	getSchema,
	type FieldType,
	OptionKind,
	VecKind,
	WrappedType,
	getDependencies,
	type Field, field as fieldDecalaration, type AbstractType, variant,
	deserialize, serialize
} from "@dao-xyz/borsh";
import * as types from "@peerbit/indexer-interface";
import { toHexString } from "@peerbit/crypto";

const SQLConversionMap: any = {
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

const WRAPPED_SIMPLE_VALUE_VARIANT = "wrapped";

export type SQLLiteValue =
	| string
	| number
	| null
	| bigint
	| Uint8Array
	| Int8Array
	| ArrayBuffer;

export type BindableValue = string | bigint | number | Uint8Array | Int8Array | ArrayBuffer | null


export const convertToSQLType = (
	value: boolean | bigint | string | number | Uint8Array,
	type?: FieldType
): BindableValue => {
	// add bigint when https://github.com/TryGhost/node-sqlite3/pull/1501 fixed

	if (type === "bool") {
		if (value != null) {
			return value ? 1 : 0;
		}
		return null;
	}
	return value as BindableValue;
};

const nullAsUndefined = (value: any) => value === null ? undefined : value;

export const convertFromSQLType = (
	value: boolean | bigint | string | number | Uint8Array,
	type?: FieldType
) => {

	if (type === "bool") {
		if (value === 0 || value === 1 || typeof value === 'boolean') {
			return value ? true : false
		}
		return nullAsUndefined(value);
	}
	if (type === 'u64') {
		return typeof value === 'number' || typeof value === 'string' ? BigInt(value) : nullAsUndefined(value);
	}
	return nullAsUndefined(value);
}

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

type SQLField = { name: string; definition: string; type: string };
type SQLConstraint = { definition: string };
export interface Table {
	name: string;
	ctor: Constructor<any>;
	primary: string;
	primaryIndex: number,
	path: string[],
	fields: SQLField[];
	constraints: SQLConstraint[];
	children: Table[];
	parent: Table | undefined
	referencedInArray: boolean,
	isSimpleValue: boolean
}


export const getSQLTable = (
	ctor: Constructor<any>,
	path: string[],
	primaryKey: string,
	addJoinField?: (fields: SQLField[], constraints: SQLConstraint[]) => void,
	/* name: string */
): Table[] => {
	let clazzes = getDependencies(ctor, 0) as any as Constructor<any>[];
	if (!clazzes) {
		clazzes = [ctor]
	}

	let ret: Table[] = [];
	for (const ctor of clazzes) {

		const name = getTableName(path, getNameOfClass(ctor));

		const newPath: string[] = [name]

		const { constraints, fields, dependencies } = getSQLFields(
			name,
			newPath,
			ctor,
			primaryKey,
			addJoinField
		);

		const table: Table = { name, constraints, fields, ctor, path: newPath, primary: primaryKey, primaryIndex: fields.findIndex(x => x.name === primaryKey), children: dependencies, parent: undefined, referencedInArray: false, isSimpleValue: false }
		ret.push(table)
		for (const dep of dependencies) {
			dep.parent = table
			// ret.push(dep)
		}


	}

	return ret;

};

const getNameOfClass = (ctor: AbstractType<any>) => {
	let name: string;
	const schema = getSchema(ctor);
	if (!schema) {
		throw new Error("Schema not found for " + ctor.name)
	}
	if (schema.variant === undefined) {
		console.warn(
			`Schema associated with ${ctor.name} has no variant.  This will results in SQL table with name generated from the Class name. This is not recommended since changing the class name will result in a new table`
		);
		name = ctor.name;
	} else {
		name =
			typeof schema.variant === "string"
				? schema.variant
				: JSON.stringify(schema.variant);
	}
	return name
}

export const getTableName = (path: string[] = [], clazz: string | Constructor<any>) => {
	let name: string = typeof clazz === 'string' ? clazz : getNameOfClass(clazz);

	// prefix the generated table name so that the name is a valid SQL identifier (table name)
	// choose prefix which is readable and explains that this is a generated table name
	const ret = path.join("__") + "__" + name.replace(/[^a-zA-Z0-9_]/g, "_");
	return ret
};


export const CHILD_TABLE_ID = "__id";
export const ARRAY_INDEX_COLUMN = "__index";

export const PARENT_TABLE_ID = "__parent_id";
const FOREIGN_VALUE_PROPERTY = "value";

interface SimpleNested { [FOREIGN_VALUE_PROPERTY]: any };

export const getSQLFields = (
	tableName: string,
	path: string[],
	ctor: Constructor<any>,
	primaryKey?: string,
	addJoinFieldFromParent?: (fields: SQLField[], constraints: SQLConstraint[]) => void,
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

	const handleNestedType = (key: string, field: VecKind | Constructor<any> | AbstractType<any>) => {
		let chilCtor: Constructor<any>;

		let elementType: any;
		let isVec = false;
		if (field instanceof VecKind) {
			if (field.elementType instanceof VecKind) {
				throw new Error("vec(vec(...)) is not supported");
			}
			elementType = field.elementType;
			isVec = true;
		}
		else {
			elementType = field;
		}


		let isSimpleValue = false;
		if (
			typeof elementType === "function" &&
			elementType != Uint8Array
		) {
			chilCtor = elementType as Constructor<any>;
		} else {
			@variant(WRAPPED_SIMPLE_VALUE_VARIANT)
			class SimpleNested implements SimpleNested {

				@fieldDecalaration({ type: elementType })
				[FOREIGN_VALUE_PROPERTY]: any;

				constructor(value: any) {
					this[FOREIGN_VALUE_PROPERTY] = value;
				}
			}
			chilCtor = SimpleNested;
			isSimpleValue = true;
		}

		const parentPrimaryFieldName = primaryField?.key || CHILD_TABLE_ID;
		const parentPrimaryFieldType = primaryField
			? toSQLType(primaryField.type)
			: "INTEGER"


		const addJoinFields = (fields: SQLField[], contstraints: SQLConstraint[]) => {

			if (isVec) {
				// add index field
				fields.unshift({
					name: ARRAY_INDEX_COLUMN,
					definition: ARRAY_INDEX_COLUMN + ' INTEGER',
					type: 'INTEGER'
				})
			}

			fields.unshift(
				{
					name: CHILD_TABLE_ID,
					definition: `${CHILD_TABLE_ID} INTEGER PRIMARY KEY`,
					type: "INTEGER"
				},

				// foreign key parent document
				{
					name: PARENT_TABLE_ID,
					definition: `${PARENT_TABLE_ID} ${parentPrimaryFieldType}`,
					type: parentPrimaryFieldType
				},

			)
			contstraints.push({
				definition: `CONSTRAINT ${PARENT_TABLE_ID}_fk FOREIGN KEY(${PARENT_TABLE_ID}) REFERENCES ${tableName}(${parentPrimaryFieldName}) ON DELETE CASCADE`
			})
		}

		const subtables = getSQLTable(chilCtor, [...path, key], CHILD_TABLE_ID, addJoinFields);
		for (const table of subtables) {
			if (!tables.find((x) => x.name === table.name)) {
				table.referencedInArray = field instanceof VecKind
				table.isSimpleValue = isSimpleValue
				tables.push(table);
			}
		}
	};

	const handleSimpleField = (key: string, type: FieldType, isOptional: boolean) => {
		const isPrimary = primaryKey === key;
		foundPrimary = foundPrimary || isPrimary;
		const fieldType = toSQLType(type, isOptional);
		sqlFields.push({
			name: key,
			definition: `'${key}' ${fieldType} ${isPrimary ? "PRIMARY KEY" : ""}`,
			type: fieldType
		});
	};

	const handleField = (key: string, type: FieldType, isOptional: boolean) => {
		if (
			typeof type === "string" ||
			type == Uint8Array
		) {
			handleSimpleField(key, type, true);
		}
		else if (typeof type === "function") {
			handleNestedType(key, type)
		} else {
			throw new Error(
				`Unsupported type in option, ${typeof type}: ${typeof type}`
			);
		}
	}

	for (const field of fields) {
		if (field.type instanceof VecKind) {
			handleNestedType(field.key, field.type);
		} else if (field.type instanceof OptionKind) {
			if (field.type.elementType instanceof VecKind) {
				// TODO
				throw new Error("option(vec(T)) not supported");
			} else if (field.type.elementType instanceof OptionKind) {
				throw new Error("option(option(T)) not supported");
			}
			handleField(field.key, field.type.elementType, true)
		} else {
			handleField(field.key, field.type, isOptional)
		}
	}
	if (!foundPrimary) {
		if (primaryKey != CHILD_TABLE_ID) {
			throw new Error(`Primary key ${primaryKey} not found in schema`);
		}
	}

	addJoinFieldFromParent?.(sqlFields, sqlConstraints)
	return {
		fields: sqlFields,
		constraints: sqlConstraints,
		dependencies: tables,
	};
};

export const resolveTable = (
	key: string[],
	tables: Map<string, Table>,
	clazz: string | Constructor<any>
) => {
	const name = /* key == null ? */ getTableName(key, clazz) /* : getSubTableName(scope, key, ctor); */
	const table = tables.get(name) || tables.get(key.join("__") + "__" + WRAPPED_SIMPLE_VALUE_VARIANT);
	if (!table) {

		throw new Error(`Table not found for ${name}: ${Array.from(tables.keys())}`);
	}
	return table;
};


const isNestedType = (type: FieldType): type is AbstractType<any> => {
	const unwrapped = unwrapNestedType(type);
	return typeof unwrapped === "function" && unwrapped !== Uint8Array;
}
const unwrapNestedType = (type: FieldType): Constructor<any> => {
	if (type instanceof WrappedType) {
		return type.elementType as Constructor<any>;
	}
	return type as Constructor<any>;


}

const getTableFromField = (parentTable: Table, tables: Map<string, Table>, field: Field) => {

	if (!field) {

		throw new Error("Field is undefined")

	}
	let clazzNames: string[] = [];
	if (!isNestedType(field.type)) {
		clazzNames.push(WRAPPED_SIMPLE_VALUE_VARIANT)
	}
	else {
		const testCtors = [unwrapNestedType(field.type), ...(getDependencies(unwrapNestedType(field.type), 0) || []) as Constructor<any>[]]
		for (const ctor of testCtors) {
			if (!ctor) {
				continue;
			}
			const schema = getSchema(ctor);
			if (!schema) {
				continue;
			}
			if (ctor) {
				clazzNames.push(getNameOfClass(ctor))
			}

		}
	}
	if (clazzNames.length === 0) {
		throw new Error("Could not find class name")
	}

	const subTable = clazzNames.map(clazzName => resolveTable([...parentTable.path, field.key], tables, clazzName));
	return subTable;



}

const getTableFromValue = (parentTable: Table, tables: Map<string, Table>, field: Field, value: any): Table => {

	try {
		let clazzName: string | undefined = undefined;
		if (!isNestedType(field.type)) {
			clazzName = WRAPPED_SIMPLE_VALUE_VARIANT
		}
		else {
			const testCtors = [/* ...(getDependencies(unwrapNestedType(field.type), 0) || []) as Constructor<any>[], */ value.constructor]
			for (const ctor of testCtors) {
				if (!ctor) {
					continue;
				}
				const schema = getSchema(ctor);
				if (!schema) {
					continue;
				}
				if (ctor) {
					clazzName = getNameOfClass(ctor);
					break;
				}

			}
		}
		if (!clazzName) {
			throw new Error("Could not find class name")
		}

		const subTable = resolveTable([...parentTable.path, field.key], tables, clazzName);
		return subTable;
	} catch (error) {
		throw error
	}


}


export const resolveFieldValues = (
	obj: Record<string, any>,
	tables: Map<string, Table>,
	table: Table,
	parentId: any = undefined,
	index?: number,
): { table: Table; values: (any | ((parentId: any) => any))[] }[] => {
	const fields = getSchema(table.ctor).fields;
	const result: { table: Table; values: any[] } = { table, values: [] };
	const ret: { table: Table; values: any[] }[] = [];


	const handleElement = (item: any, field: Field, index?: number) => {
		const subTable = getTableFromValue(table, tables, field, item);

		const itemResolved = resolveFieldValues(
			(typeof item === "function" && item instanceof Uint8Array === false) ? item : new subTable.ctor(item),
			tables,
			subTable,
			parentId ?? obj[table.primary], /* parentId ?? obj[table.primary] */
			index
		);

		ret.push(...itemResolved);
	}

	const handleNested = (field: Field, optional = false) => {
		if (Array.isArray(obj[field.key])) {
			const arr = obj[field.key];
			for (let i = 0; i < arr.length; i++) {
				const item = arr[i];
				handleElement(item, field, i);
			}
		} else {

			if (field instanceof VecKind) {
				if (obj[field.key] == null) {
					if (!optional) {
						throw new Error("Expected array, received null");
					} else {
						return;
					}
				}
				throw new Error("Expected array");
			}

			const value = obj[field.key]
			if (value == null) {
				if (!optional) {
					throw new Error("Expected object, received null")
				}
				return;
			}
			handleElement(value, field);

		}
	};
	for (const field of fields) {
		if (typeof field.type === "string" || field.type == Uint8Array) {
			result.values.push(convertToSQLType(obj[field.key], field.type));
		} else if (field.type instanceof OptionKind) {
			if (typeof field.type.elementType === "string" || field.type.elementType == Uint8Array) {
				result.values.push(
					convertToSQLType(obj[field.key], field.type.elementType)
				);
			}
			else if (field.type.elementType instanceof VecKind) {
				handleNested(field, true);
			} else {
				handleNested(field, true)
			}
		} else if (field.type instanceof VecKind) {
			handleNested(field);
		} else {
			handleNested(field);
		}
	}



	if (parentId != null) {

		if (index != null) {
			result.values.unshift(index);
		}

		result.values.unshift(parentId);
		result.values.unshift(undefined);
	}

	return [result, ...ret];
};

export const getTablePrefixedField = (table: Table, key: string, skipPrefix: boolean = false) => `${skipPrefix ? '' : table.name + "#"}${key}`
export const getTableNameFromPrefixedField = (prefixedField: string) => prefixedField.split("#")[0]

export const selectChildren = (childrenTable: Table) => "select * from " + childrenTable.name + " where " + PARENT_TABLE_ID + " = ?"
export const selectAllFields = (table: Table) => {
	let stack = [table];
	let join: Map<string, Table> = new Map();
	const fieldResolvers: string[] = []
	for (const element of stack) {
		fieldResolvers.push(...element.fields.map((field) => `${element.name}.${field.name} as '${getTablePrefixedField(element, field.name)}'`))

		for (const child of element.children) {
			if (child.referencedInArray) {
				continue;
			}
			stack.push(child)
			join.set(child.name, child)
		}
	}

	if (fieldResolvers.length === 0) {
		throw new Error("No fields found")
	}
	return { query: `SELECT ${fieldResolvers.join(", ")} FROM ${table.name}`, join };
}


// the inverse of resolveFieldValues
export const resolveInstanceFromValue = async <T>(
	fromTablePrefixedValues: Record<string, any>,
	tables: Map<string, Table>,
	table: Table,
	resolveChildren: (parentId: any, table: Table) => Promise<any[]>,
	tablePrefixed: boolean
): Promise<T> => {

	const fields = getSchema(table.ctor).fields;
	const obj: any = {};


	const handleNested = async (field: Field, isOptional: boolean, isArray: boolean) => {
		const subTables = getTableFromField(table, tables, field); // TODO fix


		if (isArray) {
			let once = false
			let resolvedArr = [];

			for (const subtable of subTables) {
				const arr = await resolveChildren(fromTablePrefixedValues[getTablePrefixedField(table, table.primary, !tablePrefixed)], subtable);
				if (arr) {
					once = true
					for (const element of arr) {
						const resolved: SimpleNested | any = await resolveInstanceFromValue(
							element,
							tables,
							subtable, // TODO fix
							resolveChildren,
							false
						);

						resolvedArr[element[ARRAY_INDEX_COLUMN]] = (subtable.isSimpleValue ? resolved.value : resolved);
					}


				}
			}


			if (!once) {
				obj[field.key] = undefined
			}
			else {
				obj[field.key] = resolvedArr;
			}


		}
		else {

			// resolve nested object from row directly 
			/* let extracted: any = {} */
			let subTable: Table | undefined = undefined
			if (subTables.length > 1) {
				for (const table of subTables) {
					if (fromTablePrefixedValues[getTablePrefixedField(table, table.primary, !tablePrefixed)] != null) {
						subTable = table
						break
					}
				}
			}
			else {
				subTable = subTables[0]
			}

			if (!subTable) {
				throw new Error("Sub table not found")
			}
			/* 
						for (const field of subTable.fields) {
							once = true
							extracted[field.name] = fromTablePrefixedValues[getTablePrefixedField(subTable, field.name, !tablePrefixed)]
						}
			 */

			if (fromTablePrefixedValues[getTablePrefixedField(subTable, subTable.primary, !tablePrefixed)] == null) {
				obj[field.key] = undefined
			}
			else {
				const resolved = await resolveInstanceFromValue(
					fromTablePrefixedValues,
					tables,
					subTable,
					resolveChildren,
					tablePrefixed
				);

				obj[field.key] = resolved;
			}

		}


	};

	for (const field of fields) {
		const fieldValue = fromTablePrefixedValues[getTablePrefixedField(table, field.key, !tablePrefixed)];
		if (typeof field.type === "string" || field.type == Uint8Array) {
			obj[field.key] = convertFromSQLType(fieldValue, field.type);
		} else if (field.type instanceof OptionKind) {
			if (typeof field.type.elementType === "string" || field.type.elementType == Uint8Array) {
				obj[field.key] = convertFromSQLType(fieldValue, field.type.elementType);
			}
			else if (field.type.elementType instanceof VecKind) {
				await handleNested(field, true, true);
			}
			else {
				await handleNested(field, true, false);

			}
		} else if (field.type instanceof VecKind) {
			await handleNested(field, false, true);
		} else {
			await handleNested(field, false, false);

		}
	}


	return Object.assign(Object.create(table.ctor.prototype), obj);
}

export const fromRowToObj = (row: any, ctor: Constructor<any>) => {
	const schema = getSchema(ctor);
	const fields = schema.fields;
	const obj: any = {};
	for (const field of fields) {
		obj[field.key] = row[field.key];
	}
	return Object.assign(Object.create(ctor.prototype), obj);
};


export const convertSumRequestToQuery = (request: types.SumRequest, tables: Map<string, Table>, table: Table) => {
	return `SELECT SUM(${table.name}.${request.key.join(".")}) as sum FROM ${table.name} ${convertRequestToQuery(request, tables, table).query}`;
}

export const convertCountRequestToQuery = (request: types.CountRequest, tables: Map<string, Table>, table: Table) => {
	return `SELECT count(*) as count FROM ${table.name} ${convertRequestToQuery(request, tables, table).query}`;
}

export const convertSearchRequestToQuery = (request: types.SearchRequest, tables: Map<string, Table>, table: Table) => {
	const { query: selectQuery, join: joinFromSelect } = selectAllFields(table)
	const { orderBy, query } = convertRequestToQuery(request, tables, table, joinFromSelect)
	return `${selectQuery} ${query} ${orderBy ? orderBy : ""} limit ? offset ?`;

}


type SearchQueryParts = { query: string; orderBy: string }
type CountQueryParts = { query: string; join: string }

const convertRequestToQuery = <T extends (types.SearchRequest | types.CountRequest | types.SumRequest), R = T extends types.SearchRequest ? SearchQueryParts : CountQueryParts>(
	request: T,
	tables: Map<string, Table>,
	table: Table,
	extraJoin?: Map<string, Table>
): R => {
	let whereBuilder = "";
	let joinBuilder = new Map<string, Table>();
	let orderByBuilder: string | undefined = undefined;
	/* let tablesToSelect: string[] = [table.name]; */

	if (request.query.length === 1) {
		const { where, join } = convertQueryToSQLQuery(
			request.query[0],
			tables,
			table
		);
		whereBuilder += where;
		if (join) {
			insertMapIntoMap(joinBuilder, join)
		}
	} else if (request.query.length > 1) {
		const { where, join } = convertQueryToSQLQuery(
			new types.And(request.query),
			tables,
			table
		);
		whereBuilder += where;
		if (join) {
			insertMapIntoMap(joinBuilder, join)
		}
	}

	if (request instanceof types.SearchRequest) {
		if (request.sort.length > 0) {
			if (request.sort.length > 0) {
				orderByBuilder = "ORDER BY ";
			}
			let once = false
			for (const sort of request.sort) {
				try {
					const { foreignTables, join, queryKey } = resolveTableToQuery(table, tables, sort.key);
					insertMapIntoMap(joinBuilder, join);
					for (const table of foreignTables) {
						if (once) {
							orderByBuilder += ", "
						}
						once = true;
						orderByBuilder += `${table.name}.${queryKey} ${sort.direction === types.SortDirection.ASC ? "ASC" : "DESC"}`
					}
				} catch (error) {
					throw error;
				}
			}

			/* orderByBuilder += request.sort
				.map(
					(sort) =>
						`${table.name}.${sort.key} ${sort.direction === types.SortDirection.ASC ? "ASC" : "DESC"}`
				)
				.join(", "); */
		}
	}
	const where = whereBuilder.length > 0 ? "where " + whereBuilder : undefined;


	if (extraJoin) {
		insertMapIntoMap(joinBuilder, extraJoin)
	}
	let join = buildJoin(joinBuilder, request instanceof types.SearchRequest ? true : false);

	const query = `${join ? join : ""} ${where ? where : ""}`;

	return {
		query,
		orderBy: orderByBuilder
	} as R;
};

export const buildJoin = (joinBuilder: Map<string, Table>, resolveAllColumns: boolean) => {
	let joinTypeDefault = resolveAllColumns ? "FULL OUTER JOIN" : "JOIN";
	let join = ""
	for (const [_key, table] of joinBuilder) {
		if (!table.parent) {
			throw new Error("Unexpected: missing parent")
		}
		let joinType = table.referencedInArray ? "FULL OUTER JOIN" : joinTypeDefault;

		join += `${joinType} ${table.name} ON ${table.parent.name}.${table.parent.primary} = ${table.name}.${PARENT_TABLE_ID} `
	}
	return join;
}

const insertMapIntoMap = (map: Map<string, any>, insert: Map<string, any>) => {
	for (const [key, value] of insert) {
		map.set(key, value);
	}
}

export const convertQueryToSQLQuery = (
	query: types.Query,
	tables: Map<string, Table>,
	table: Table
): { where: string; join?: Map<string, Table> } => {
	let whereBuilder = "";
	let joinBuilder = new Map<string, Table>()
	/* 	let tablesToSelect: string[] = []; */

	if (query instanceof types.StateFieldQuery) {
		const { where, join } = convertStateFieldQuery(query, tables, table);
		whereBuilder += where;
		join && insertMapIntoMap(joinBuilder, join)
	} else if (query instanceof types.LogicalQuery) {
		if (query instanceof types.And) {
			for (const subquery of query.and) {
				const { where, join } = convertQueryToSQLQuery(subquery, tables, table);
				whereBuilder =
					whereBuilder.length > 0 ? `(${whereBuilder}) AND (${where})` : where;
				join && insertMapIntoMap(joinBuilder, join)
			}
		} else if (query instanceof types.Or) {
			for (const subquery of query.or) {
				const { where, join } = convertQueryToSQLQuery(subquery, tables, table);
				whereBuilder =
					whereBuilder.length > 0 ? `(${whereBuilder}) OR (${where})` : where;
				join && insertMapIntoMap(joinBuilder, join)
			}
		}
		else if (query instanceof types.Not) {
			const { where, join } = convertQueryToSQLQuery(query.not, tables, table);
			whereBuilder = `NOT (${where})`;
			join && insertMapIntoMap(joinBuilder, join)
		}
		else {
			throw new Error("Unsupported query type: " + query.constructor.name);
		}
	} else {
		throw new Error("Unsupported query type: " + query.constructor.name);
	}

	return {
		where: whereBuilder,
		join: joinBuilder.size > 0 ? joinBuilder : undefined
	};
};

const cloneQuery = (query: types.StateFieldQuery) => {
	return deserialize(serialize(query), types.StateFieldQuery);
};

const resolveTableToQuery = (table: Table, tables: Map<string, Table>, path: string[]) => {
	const joins = new Map<string, Table>()
	let foreignTables: Table[] = [table];
	let queryKey = FOREIGN_VALUE_PROPERTY;


	outer:
	for (const [i, key] of path.entries()) {
		const currentTables = foreignTables || [table];
		for (const currentTable of currentTables) {
			if (currentTable.fields.find(x => x.name === key) && i === path.length - 1) {
				queryKey = key;
				break outer;
			}

			const schema = getSchema(currentTable.ctor);
			const field = schema.fields.find((x) => x.key === key)!;
			foreignTables = getTableFromField(currentTable, tables, field)
			for (const foreignTable of foreignTables) {
				joins.set(foreignTable.name, foreignTable)
			}
		}

		if (i === path.length - 2) {
			const foreignTablesWithField = foreignTables.filter(t => t.fields.find((x) =>
				x.name === path[i + 1])
			)
			if (foreignTablesWithField.length > 0) {
				queryKey = path[i + 1];
				foreignTables = foreignTablesWithField;
				break;
			}
		}
	}

	return { join: joins, queryKey, foreignTables };
}



const convertStateFieldQuery = (
	query: types.StateFieldQuery,
	tables: Map<string, Table>,
	table: Table
): { join?: Map<string, Table>; where: string } => {
	// if field id represented as foreign table, do join and compare
	const isForeign = query.key.length > 1 || !table.fields.find(x => x.name === query.key[query.key.length - 1])
	if (isForeign) {
		const { join, queryKey, foreignTables } = resolveTableToQuery(table, tables, query.key);
		query = cloneQuery(query);
		query.key = [queryKey];
		let whereBuilder: string[] = []
		for (const table of foreignTables) {
			const { where } = convertQueryToSQLQuery(query, tables, table);
			whereBuilder.push(where);
		}
		return { where: whereBuilder.join(" OR "), join };
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
		if (table.fields.find(x => x.name === query.key[0]!)!.type === "BLOB") {
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
	} else if (query instanceof types.IsNull) {
		where = `${keyWithTable} IS NULL`;
	} else if (query instanceof types.BoolQuery) {
		where = `${keyWithTable} = ${query.value}`;
	} else {
		throw new Error("Unsupported query type: " + query.constructor.name);
	}
	return { where };
};
