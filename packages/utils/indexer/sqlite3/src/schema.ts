import {
	type AbstractType,
	type Constructor,
	type Field,
	type FieldType,
	FixedArrayKind,
	OptionKind,
	VecKind,
	WrappedType,
	deserialize,
	field as fieldDecalaration,
	getDependencies,
	getSchema,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import { fromHexString, toHexString } from "@peerbit/crypto";
import * as types from "@peerbit/indexer-interface";
import { type PlanningSession, flattenQuery } from "./query-planner.js";

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
	Date: "TEXT",
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

export type BindableValue =
	| string
	| bigint
	| number
	| Uint8Array
	| Int8Array
	| ArrayBuffer
	| null;

let JSON_GROUP_ARRAY = "json_group_array";
let JSON_OBJECT = "distinct json_object";

const coerceLocalQuery = (query: types.Query): types.Query => {
	if (query instanceof types.Query) {
		return query;
	}
	return deserialize(serialize(query), types.Query);
};

export const coerceLocalQueries = (
	query?:
		| types.Query[]
		| types.Query
		| Record<string, string | number | bigint | Uint8Array | boolean | null | undefined>,
): types.Query[] => {
	if (!query) {
		return [];
	}
	if (Array.isArray(query)) {
		return query.map((entry) => coerceLocalQuery(entry));
	}
	if (query instanceof types.Query) {
		return [query];
	}
	try {
		return [coerceLocalQuery(query as unknown as types.Query)];
	} catch {
		return types.toQuery(query).map((entry) => coerceLocalQuery(entry));
	}
};

export const coerceLocalSorts = (
	sort?: types.Sort[] | types.Sort,
): types.Sort[] | types.Sort | undefined => {
	if (!sort) {
		return undefined;
	}
	if (Array.isArray(sort)) {
		return sort.map((entry) =>
			entry instanceof types.Sort
				? entry
				: deserialize(serialize(entry), types.Sort),
		);
	}
	return sort instanceof types.Sort
		? sort
		: deserialize(serialize(sort), types.Sort);
};

export const u64ToI64 = (u64: bigint | number) => {
	return (typeof u64 === "number" ? BigInt(u64) : u64) - 9223372036854775808n;
};
export const i64ToU64 = (i64: number | bigint) =>
	(typeof i64 === "number" ? BigInt(i64) : i64) + 9223372036854775808n;

export const convertToSQLType = (
	value: boolean | bigint | string | number | Uint8Array,
	type?: FieldType,
): BindableValue => {
	// add bigint when https://github.com/TryGhost/node-sqlite3/pull/1501 fixed

	if (value != null) {
		if (type === "bool") {
			return value ? 1 : 0;
		}
		if (type === "u64") {
			// shift to fit in i64

			return u64ToI64(value as number | bigint);
		}
	}
	return value as BindableValue;
};

const nullAsUndefined = (value: any) => (value === null ? undefined : value);
export const escapeColumnName = (name: string, char = '"') =>
	`${char}${name}${char}`;

export class MissingFieldError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "MissingFieldError";
	}
}

export const convertFromSQLType = (
	value: boolean | bigint | string | number | Uint8Array,
	type: FieldType | undefined,
) => {
	if (type === "bool") {
		if (
			value === 0 ||
			value === 1 ||
			value === 0n ||
			value === 1n ||
			typeof value === "boolean"
		) {
			return value ? true : false;
		}
		return nullAsUndefined(value);
	}
	if (type === "u8" || type === "u16" || type === "u32") {
		return typeof value === "bigint" || typeof value === "string"
			? Number(value)
			: nullAsUndefined(value);
	}
	if (type === "u64") {
		if (typeof value === "number" || typeof value === "bigint") {
			return i64ToU64(value as number | bigint); // TODO is not always value type bigint?
		}
		if (value == null) {
			return nullAsUndefined(value);
		}
		throw new Error(
			`Unexpected value type for value ${value} expected number or bigint for u64 field`,
		);
	}
	return nullAsUndefined(value);
};

export const toSQLType = (type: FieldType, isOptional = false) => {
	let ret: string;
	if (typeof type === "string") {
		const sqlType = SQLConversionMap[type];
		if (!sqlType) {
			throw new Error(`Type ${type} is not supported in SQL`);
		}
		ret = sqlType;
	} else if (isUint8ArrayType(type)) {
		ret = "BLOB";
	} else if (type instanceof OptionKind) {
		throw new Error("Unexpected option");
	} else if (type instanceof VecKind) {
		throw new Error("Unexpected vec");
	} else {
		throw new Error(`Type ${JSON.stringify(type)} is not supported in SQL`);
	}

	return isOptional ? ret : ret + " NOT NULL";
};

type SQLField = {
	name: string;
	key: string;
	definition: string;
	type: string;
	isPrimary: boolean;
	from: Field | undefined;
	unwrappedType: FieldType | undefined;
	path: string[];
	describesExistenceOfAnother?: string;
};
type SQLConstraint = { name: string; definition: string };
type PrimaryFieldInfo = Pick<
	SQLField,
	"name" | "type" | "from" | "unwrappedType"
>;

const createScalarSQLField = (
	path: string[],
	field: Field,
	type: FieldType,
	primary: string | false,
	isOptional: boolean,
): SQLField => {
	const name = getInlineTableFieldName(path.slice(1), field.key);
	const isPrimary = primary !== false && name === primary;
	const sqlType = toSQLType(type, isOptional);
	return {
		name,
		key: field.key,
		definition: `${escapeColumnName(name)} ${sqlType} ${isPrimary ? "PRIMARY KEY" : ""}`,
		type: sqlType,
		isPrimary,
		from: field,
		unwrappedType: unwrapNestedType(field.type),
		path: [...path.slice(1), field.key],
	};
};

const resolvePrimaryFieldInfoFromSchema = (
	ctor: Constructor<any>,
	path: string[],
	primary: string,
): PrimaryFieldInfo | undefined => {
	const schema = getSchema(ctor);
	if (!schema) {
		return undefined;
	}

	for (const field of schema.fields) {
		let fieldType: FieldType = field.type;

		// option(T) is stored as T (nullable) in SQL.
		if (fieldType instanceof OptionKind) {
			fieldType = fieldType.elementType;
		}

		// fixedArray(u8, N) represents bytes and must map to BLOB in SQL. Note that
		// FixedArrayKind is also a WrappedType, so unwrapNestedType() would otherwise
		// turn it into the scalar "u8" and incorrectly treat it as INTEGER.
		if (fieldType instanceof FixedArrayKind && fieldType.elementType === "u8") {
			fieldType = Uint8Array;
		} else {
			fieldType = unwrapNestedType(fieldType);
		}

		// Arrays are always stored in separate tables.
		if (fieldType instanceof VecKind) {
			continue;
		}

		if (typeof fieldType === "string" || isUint8ArrayType(fieldType)) {
			const sqlField = createScalarSQLField(
				path,
				field,
				fieldType,
				primary,
				true,
			);
			if (sqlField.isPrimary) {
				return {
					name: sqlField.name,
					type: sqlField.type,
					from: sqlField.from,
					unwrappedType: sqlField.unwrappedType,
				};
			}
		} else if (
			typeof fieldType === "function" &&
			clazzCanBeInlined(fieldType as Constructor<any>)
		) {
			const nested = resolvePrimaryFieldInfoFromSchema(
				fieldType as Constructor<any>,
				[...path, field.key],
				primary,
			);
			if (nested) {
				return nested;
			}
		}
	}

	return undefined;
};

export interface Table {
	name: string;
	ctor: Constructor<any>;
	primary: string | false;
	primaryIndex: number; // can be -1 for nested tables TODO make it more clear
	primaryField?: SQLField; // can be undefined for nested tables TODO make it required
	path: string[];
	parentPath: string[] | undefined; // field path of the parent where this table originates from
	fields: SQLField[];
	constraints: SQLConstraint[];
	children: Table[];
	inline: boolean;
	parent: Table | undefined;
	referencedInArray: boolean;
	isSimpleValue: boolean;
	indices: Set<string>;
}

export const getSQLTable = (
	ctor: AbstractType<any>,
	path: string[],
	primary: string | false,
	inline: boolean,
	addJoinField:
		| ((fields: SQLField[], constraints: SQLConstraint[]) => void)
		| undefined,
	fromOptionalField: boolean = false,

	/* name: string */
): Table[] => {
	let clazzes = getDependencies(ctor, 0) as any as Constructor<any>[];
	if (!clazzes || clazzes.length === 0) {
		clazzes = [ctor as Constructor<any>];
	}

	let ret: Table[] = [];
	for (const ctor of clazzes) {
		const name = getTableName(path, getNameOfClass(ctor));
		const newPath: string[] = inline ? path : [name];
		const { constraints, fields, dependencies } = getSQLFields(
			name,
			newPath,
			ctor,
			primary,
			addJoinField,
			[],
			fromOptionalField,
		);

		const table: Table = {
			name,
			constraints,
			fields,
			ctor,
			parentPath: path,
			path: newPath,
			primaryField: fields.find((x) => x.isPrimary)!,
			primary,
			primaryIndex: fields.findIndex((x) => x.isPrimary),
			children: dependencies,
			parent: undefined,
			referencedInArray: false,
			isSimpleValue: false,
			inline,
			indices: new Set<string>(),
		};
		ret.push(table);
		for (const dep of dependencies) {
			dep.parent = table;
			// ret.push(dep)
		}
	}

	return ret;
};

const getNameOfVariant = (variant: any) => {
	return (
		"v_" + (typeof variant === "string" ? variant : JSON.stringify(variant))
	);
};

const getNameOfClass = (ctor: AbstractType<any>) => {
	let name: string;
	const schema = getSchema(ctor);
	if (!schema) {
		throw new Error("Schema not found for " + ctor.name);
	}
	if (schema.variant === undefined) {
		const ctorName = ctor.name || "<anonymous>";
		const variantHint = ctor.name || "your-variant";
		throw new Error(
			`Schema associated with ${ctorName} has no variant. Add @variant("${variantHint}") to define a stable table name.`,
		);
	} else {
		name = getNameOfVariant(schema.variant);
	}
	return name;
};

export const getTableName = (
	path: string[] = [],
	clazz: string | Constructor<any>,
) => {
	let pathKey = path.length > 0 ? path.join("__") + "__" : "";
	if (typeof clazz !== "string") {
		const tableName = (clazz as any)["__table_" + pathKey];
		if (tableName) {
			return tableName;
		}
	}

	let name: string = typeof clazz === "string" ? clazz : getNameOfClass(clazz);

	// prefix the generated table name so that the name is a valid SQL identifier (table name)
	// choose prefix which is readable and explains that this is a generated table name

	// leading _ to allow path to have numbers

	const ret = pathKey + name.replace(/[^a-zA-Z0-9_]/g, "_");

	if (typeof clazz !== "string") {
		(clazz as any)["__table_" + pathKey] = ret;
	}
	return ret;
};

export const CHILD_TABLE_ID = "__id";
export const ARRAY_INDEX_COLUMN = "__index";

export const PARENT_TABLE_ID = "__parent_id";
const FOREIGN_VALUE_PROPERTY = "value";

const clazzCanBeInlined = (clazz: Constructor<any>) => {
	return (getDependencies(clazz, 0)?.length ?? 0) === 0;
};

interface SimpleNested {
	[FOREIGN_VALUE_PROPERTY]: any;
}

const getInlineObjectExistColumnName = () => {
	return "_exist";
};

export const getSQLFields = (
	tableName: string,
	path: string[],
	ctor: Constructor<any>,
	primary: string | false,
	addJoinFieldFromParent?: (
		fields: SQLField[],
		constraints: SQLConstraint[],
	) => void,
	tables: Table[] = [],
	isOptional = false,
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

	// Resolve the primary field info independent of schema field order. This is
	// needed because nested table generation can happen before the primary field
	// has been processed and added to `sqlFields`.
	const parentPrimaryFieldInfo: PrimaryFieldInfo =
		primary === false || primary === CHILD_TABLE_ID
			? {
					name: CHILD_TABLE_ID,
					type: "INTEGER",
					from: undefined,
					unwrappedType: undefined,
				}
			: resolvePrimaryFieldInfoFromSchema(ctor, path, primary) || {
					// Fallback: nested tables use synthetic integer ids, and we allow that
					// primary to be unresolved here.
					name: primary,
					type: "INTEGER",
					from: undefined,
					unwrappedType: undefined,
				};

	const addJoinFields =
		primary === false
			? addJoinFieldFromParent
			: (fields: SQLField[], contstraints: SQLConstraint[]) => {
					fields.unshift(
						{
							name: CHILD_TABLE_ID,
							key: CHILD_TABLE_ID,
							definition: `${CHILD_TABLE_ID} INTEGER PRIMARY KEY`,
							type: "INTEGER",
							isPrimary: true,
							from: undefined,
							unwrappedType: undefined,
							path: [CHILD_TABLE_ID],
						},

						// foreign key parent document
						{
							name: PARENT_TABLE_ID,
							key: PARENT_TABLE_ID,
							definition: `${PARENT_TABLE_ID} ${parentPrimaryFieldInfo.type}`,
							type: parentPrimaryFieldInfo.type,
							from: parentPrimaryFieldInfo.from,
							unwrappedType: parentPrimaryFieldInfo.unwrappedType,
							isPrimary: false,
							path: [PARENT_TABLE_ID],
						},
					);
					contstraints.push({
						name: `${PARENT_TABLE_ID}_fk`,
						definition: `CONSTRAINT ${PARENT_TABLE_ID}_fk FOREIGN KEY(${PARENT_TABLE_ID}) REFERENCES ${tableName}(${parentPrimaryFieldInfo.name}) ON DELETE CASCADE`,
					});
				};

	const handleNestedType = (
		key: string,
		field: VecKind | Constructor<any> | AbstractType<any>,
	) => {
		let chilCtor: Constructor<any>;

		let elementType: any;
		let isVec = false;
		if (field instanceof VecKind) {
			if (field.elementType instanceof VecKind) {
				throw new Error("vec(vec(...)) is not supported");
			}
			elementType = field.elementType;
			isVec = true;
		} else {
			elementType = field;
		}

		let isSimpleValue = false;
		if (typeof elementType === "function" && !isUint8ArrayType(elementType)) {
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

		const subtables = getSQLTable(
			chilCtor,
			[...path, key],
			CHILD_TABLE_ID,
			false,
			addJoinFields,
		);

		for (const table of subtables) {
			if (!tables.find((x) => x.name === table.name)) {
				if (isVec) {
					table.referencedInArray = true;

					table.fields = [
						...table.fields.slice(0, 2),
						{
							name: ARRAY_INDEX_COLUMN,
							key: ARRAY_INDEX_COLUMN,
							definition: ARRAY_INDEX_COLUMN + " INTEGER",
							type: "INTEGER",
							isPrimary: false,
							from: undefined,
							unwrappedType: undefined,
							path: [ARRAY_INDEX_COLUMN],
						},
						...table.fields.slice(2),
					];
				}
				table.isSimpleValue = isSimpleValue;
				tables.push(table);
			}
		}
	};

	const handleSimpleField = (
		field: Field,
		type: FieldType,
		isOptional: boolean,
	) => {
		const sqlField = createScalarSQLField(
			path,
			field,
			type,
			primary,
			isOptional,
		);
		foundPrimary = foundPrimary || sqlField.isPrimary;
		sqlFields.push(sqlField);
	};

	const handleField = (
		key: string,
		field: Field,
		type: FieldType,
		isOptional: boolean,
	) => {
		if (type instanceof FixedArrayKind && type.elementType === "u8") {
			type = Uint8Array;
		}

		if (typeof type === "string" || type === Uint8Array) {
			handleSimpleField(field, type, true);
		} else if (
			typeof type === "function" &&
			clazzCanBeInlined(type as Constructor<any>)
		) {
			// if field is object but is not polymorphic we can do a simple field inlining

			const subPath = [...path, key];
			const subtables = getSQLTable(
				type as Constructor<any>,
				subPath,
				false,
				true,
				addJoinFields,
				isOptional,
			);
			for (const table of subtables) {
				if (!tables.find((x) => x.name === table.name)) {
					tables.push(table);
					if (table.inline) {
						for (const field of table.fields) {
							const isPrimary = primary != null && field.name === primary;
							foundPrimary = foundPrimary || isPrimary;
							sqlFields.push(field);
						}
						sqlConstraints.push(...table.constraints);
					}
				}
			}
		} else if (typeof type === "function") {
			handleNestedType(key, type);
		} else {
			throw new Error(`Unsupported type: ${JSON.stringify(type)}}`);
		}
	};

	for (const field of fields) {
		if (field.type instanceof VecKind) {
			handleNestedType(field.key, field.type);
		} else if (field.type instanceof OptionKind) {
			if (field.type.elementType instanceof VecKind) {
				// TODO but how ?
				throw new Error("option(vec(T)) not supported");
			} else if (field.type.elementType instanceof OptionKind) {
				throw new Error("option(option(T)) not supported");
			}
			handleField(field.key, field, field.type.elementType, true);
		} else {
			handleField(field.key, field, field.type, isOptional);
		}
	}

	if (primary !== false) {
		// primareKey will be false for nested objects that are inlined
		if (!foundPrimary && primary !== CHILD_TABLE_ID) {
			throw new Error(`Primary key ${primary} not found in schema`);
		}
		addJoinFieldFromParent?.(sqlFields, sqlConstraints);
	} else {
		// inline
		if (isOptional) {
			// add field indicating if the inline object exists,
			let key = getInlineObjectExistColumnName();
			let keyString = getInlineTableFieldName(path.slice(1), key);

			sqlFields.push({
				name: keyString,
				key,
				definition: `${escapeColumnName(keyString)} INTEGER`,
				type: "bool",
				isPrimary: false,
				from: undefined,
				unwrappedType: undefined,
				path: [...path.slice(1), key],
				describesExistenceOfAnother: path[path.length - 1],
			});
		}
	}

	return {
		fields: sqlFields,
		constraints: sqlConstraints,
		dependencies: tables,
	};
};

export const resolveTable = <
	B extends boolean,
	R = B extends true ? Table : Table | undefined,
>(
	key: string[],
	tables: Map<string, Table>,
	clazz: string | Constructor<any>,
	throwOnMissing: B,
): R => {
	const name = /* key == null ? */ getTableName(
		key,
		clazz,
	); /* : getSubTableName(scope, key, ctor); */
	const table =
		tables.get(name) ||
		tables.get(
			getTableName(
				key,
				getNameOfVariant(WRAPPED_SIMPLE_VALUE_VARIANT),
			) /* key.join("__") + "__" + getNameOfVariant(WRAPPED_SIMPLE_VALUE_VARIANT) */,
		);
	if (!table && throwOnMissing) {
		throw new Error(
			`Table not found for ${name}. Got tables: ${Array.from(tables.keys())}`,
		);
	}
	return table as R;
};

const isNestedType = (type: FieldType): type is AbstractType<any> => {
	const unwrapped = unwrapNestedType(type);
	return typeof unwrapped === "function" && unwrapped !== Uint8Array;
};
const unwrapNestedType = (type: FieldType): FieldType => {
	if (type instanceof WrappedType) {
		return type.elementType;
	}
	return type;
};

const getTableFromField = (
	parentTable: Table,
	tables: Map<string, Table>,
	field: Field,
) => {
	if (!field) {
		throw new Error("Field is undefined");
	}
	let clazzNames: string[] = [];
	if (!isNestedType(field.type)) {
		clazzNames.push(WRAPPED_SIMPLE_VALUE_VARIANT);
	} else {
		const testCtors: any[] = [
			unwrapNestedType(field.type),
			...((getDependencies(unwrapNestedType(field.type) as any, 0) ||
				[]) as Constructor<any>[]),
		];
		for (const ctor of testCtors) {
			if (!ctor) {
				continue;
			}
			const schema = getSchema(ctor);
			if (!schema) {
				continue;
			}
			if (ctor) {
				clazzNames.push(getNameOfClass(ctor));
			}
		}
	}
	if (clazzNames.length === 0) {
		throw new Error("Could not find class name");
	}

	const subTable = clazzNames
		.map((clazzName) =>
			resolveTable([...parentTable.path, field.key], tables, clazzName, false),
		)
		.filter((x) => x != null);
	return subTable;
};

const getTableFromValue = (
	parentTable: Table,
	tables: Map<string, Table>,
	field: Field,
	value?: any,
): Table => {
	let clazzName: string | Constructor<any> | undefined = undefined;
	if (!isNestedType(field.type)) {
		clazzName = WRAPPED_SIMPLE_VALUE_VARIANT;
	} else {
		const nestedType = unwrapNestedType(field.type) as Constructor<any>;
		const dependencies = (getDependencies(nestedType as any, 0) ||
			[]) as Constructor<any>[];
		const candidatesFromSchema = [nestedType, ...dependencies].filter(
			(x): x is Constructor<any> => typeof x === "function",
		);

		const runtimeCtor = value?.constructor as Constructor<any> | undefined;
		const runtimeSchema = runtimeCtor ? getSchema(runtimeCtor) : undefined;

		// If the runtime value is a proper borsh-decorated instance, use it.
		if (runtimeCtor && runtimeSchema?.variant !== undefined) {
			clazzName = runtimeCtor;
		} else {
			// Otherwise fall back to the declared field type, but only if it maps to
			// a single concrete variant/table. For polymorphic base types, a POJO
			// does not contain enough information to determine the correct table.
			const tableCtors = candidatesFromSchema.filter((ctor) => {
				const schema = getSchema(ctor);
				return schema?.variant !== undefined;
			});

			if (tableCtors.length === 1) {
				clazzName = tableCtors[0];
			} else if (tableCtors.length > 1) {
				const declaredName = nestedType.name || "<anonymous>";
				const variants = tableCtors
					.map((ctor) => {
						const schema = getSchema(ctor);
						return `${ctor.name || "<anonymous>"}(${JSON.stringify(schema?.variant)})`;
					})
					.join(", ");
				const runtimeName =
					runtimeCtor?.name || (value == null ? "null" : typeof value);
				throw new Error(
					`Ambiguous polymorphic nested value for field "${field.key}". ` +
						`Declared type ${declaredName} has multiple variants: ${variants}. ` +
						`Received ${runtimeName}. Pass an instance of the desired variant.`,
				);
			}
		}
	}
	if (!clazzName) {
		throw new Error("Could not find class name");
	}

	const subTable = resolveTable(
		[...parentTable.path, field.key],
		tables,
		clazzName,
		true,
	);
	return subTable;
};

const isUint8ArrayType = (type: FieldType) => {
	if (type === Uint8Array) {
		return true;
	}
	if (type instanceof FixedArrayKind) {
		return type.elementType === "u8";
	}
	return false;
};

export const insert = async (
	insertFn: (
		values: any[],
		table: Table,
		options?: { requireId?: boolean },
	) => Promise<any> | any,
	obj: Record<string, any>,
	tables: Map<string, Table>,
	table: Table,
	fields: Field[],
	handleNestedCallback?: (
		cb: (parentId: any) => Promise<void>,
	) => Promise<void> | void | number,
	parentId: any = undefined,
	index?: number,
	options?: {
		insertSimpleVecRows?: (
			rows: any[][],
			table: Table,
		) => Promise<void> | void;
	},
): Promise<void> => {
	const bindableValues: any[] = [];
	let nestedCallbacks: ((id: any) => Promise<void>)[] = [];

	handleNestedCallback =
		table.primary === false
			? handleNestedCallback
			: (fn) => nestedCallbacks.push(fn);

	const toInsertValue = (item: any, subTable: Table) =>
		typeof item === "function" && item instanceof Uint8Array === false
			? item
			: subTable.isSimpleValue
				? // eslint-disable-next-line new-cap
					new subTable.ctor(item)
				: Object.assign(Object.create(subTable.ctor.prototype), item);

	const handleElement = async (
		item: any,
		field: Field,
		parentId: any,
		index?: number,
	) => {
		const subTable = getTableFromValue(table, tables, field, item);

		await insert(
			insertFn,
			toInsertValue(item, subTable),
			tables,
			subTable,
			getSchema(subTable.ctor).fields,
			handleNestedCallback,
			parentId,
			index,
			options,
		);
	};

	const handleNested = async (
		field: Field,
		optional: boolean,
		parentId: any,
	) => {
		if (Array.isArray(obj[field.key])) {
			const arr = obj[field.key];
			const firstItem = arr.find((item: any) => item != null);
			if (
				parentId != null &&
				arr.length > 1 &&
				firstItem != null &&
				options?.insertSimpleVecRows
			) {
				const subTable = getTableFromValue(table, tables, field, firstItem);
				if (subTable.isSimpleValue) {
					const rows: any[][] = [];
					for (let i = 0; i < arr.length; i++) {
						const item = arr[i];
						await insert(
							(values) => {
								rows.push(values);
								return undefined;
							},
							toInsertValue(item, subTable),
							tables,
							subTable,
							getSchema(subTable.ctor).fields,
							handleNestedCallback,
							parentId,
							i,
							options,
						);
					}
					if (rows.length > 0) {
						await options.insertSimpleVecRows(rows, subTable);
					}
					return;
				}
			}
			for (let i = 0; i < arr.length; i++) {
				const item = arr[i];
				await handleElement(item, field, parentId, i);
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

			const value = obj[field.key];
			if (value == null) {
				if (!optional) {
					throw new Error("Expected object, received null");
				}
				return;
			}
			await handleElement(value, field, parentId);
		}
	};

	let nestedFields: Field[] = [];
	if (parentId != null) {
		bindableValues.push(undefined);
		bindableValues.push(parentId);
		if (index != null) {
			bindableValues.push(index);
		}
	}

	for (const field of fields) {
		const unwrappedType = unwrapNestedType(field.type);
		if (field.type instanceof VecKind === false) {
			if (
				typeof unwrappedType === "string" ||
				isUint8ArrayType(unwrappedType)
			) {
				bindableValues.push(convertToSQLType(obj[field.key], unwrappedType));
			} else if (
				typeof unwrappedType === "function" &&
				clazzCanBeInlined(unwrappedType as Constructor<any>)
			) {
				const value = obj[field.key];
				const subTable = getTableFromValue(table, tables, field, value);
				if (subTable.inline && value == null) {
					for (const _field of subTable.fields) {
						bindableValues.push(null);
					}
					bindableValues[bindableValues.length - 1] = 0; // assign the value "false" to the exist field column
					continue;
				}

				await insert(
					(values, table) => {
						if (table.inline) {
							bindableValues.push(...values); // insert the bindable values into the parent bindable array
							if (field.type instanceof OptionKind) {
								bindableValues.push(1); // assign the value "true" to the exist field column
							}
							return undefined;
						} else {
							return insertFn(values, table);
						}
					},
					value,
					tables,
					subTable,
					getSchema(unwrappedType).fields,
					(fn) => nestedCallbacks.push(fn),
					undefined, // parentId is not defined here, we are inserting a nested object inline
					undefined, // index is not defined here, we are inserting a nested object inline
					options,
				);
				/* await insert(, obj[field.key], tables, subTable, getSchema(unwrappedType).fields, parentId, index); */
			} else {
				nestedFields.push(field);
			}
		} else {
			nestedFields.push(field);
		}
	}

	// we handle nested after self insertion so we have a id defined for 'this'
	// this is important because if we insert a related document in a foreign table
	// we need to know the id of the parent document to insert the foreign key correctly
	for (const nested of nestedFields) {
		const isOptional = nested.type instanceof OptionKind;
		await handleNestedCallback!((id) => handleNested(nested, isOptional, id));
	}

	const thisId = await insertFn(bindableValues, table, {
		requireId: nestedCallbacks.length > 0,
	});
	if (table.primary === false && nestedCallbacks.length > 0) {
		throw new Error("Unexpected");
	}
	const nestedWrites = nestedCallbacks.map((write) => write(thisId));
	try {
		await Promise.all(nestedWrites);
	} catch (error) {
		// Do not release the caller's transaction while a sibling can still write.
		await Promise.allSettled(nestedWrites);
		throw error;
	}

	/* return [result, ...ret]; */
};

export const getTablePrefixedField = (
	table: Table,
	key: string,
	skipPrefix: boolean = false,
) =>
	`${skipPrefix ? "" : table.name + "#"}${getInlineTableFieldName(table.path.slice(1), key)}`;
export const getTableNameFromPrefixedField = (prefixedField: string) =>
	prefixedField.split("#")[0];

export const getInlineTableFieldName = (
	path: string[] | string | undefined,
	key?: string,
): string => {
	if (key) {
		if (Array.isArray(path)) {
			return path && path.length > 0 ? `${path.join("_")}__${key}` : key;
		}
		return path + "__" + key;
	} else {
		// last element in the path is the key, the rest is the path
		// join key with __ , rest with _

		if (!Array.isArray(path)) {
			if (!path) {
				throw new Error("Unexpected missing path");
			}
			return path;
		}

		return path!.length > 2
			? `${path!.slice(0, -1).join("_")}__${path![path!.length - 1]}`
			: path!.join("__");
	}
};

const matchFieldInShape = (
	shape: types.Shape | undefined,
	path: string[] | string | undefined,
	field: SQLField,
) => {
	if (!shape) {
		return true;
	}
	let currentShape = shape;

	if (field.path) {
		for (let i = 0; i < field.path.length; i++) {
			if (!currentShape) {
				return false;
			}
			let nextShape = currentShape[field.path[i]];
			if (nextShape === undefined) {
				return false;
			}
			if (nextShape === true) {
				return true;
			}
			if (Array.isArray(nextShape)) {
				currentShape = nextShape[0];
			} else {
				currentShape = nextShape;
			}
		}
	}

	throw new Error("Unexpected");
};

export const selectChildren = (childrenTable: Table) =>
	"select * from " + childrenTable.name + " where " + PARENT_TABLE_ID + " = ?";

export const generateSelectQuery = (
	table: Table,
	selects: { from: string; as: string }[],
) => {
	return `select ${selects.map((x) => `${x.from} as ${x.as}`).join(", ")} FROM ${table.name}`;
};

export const selectAllFieldsFromTables = (
	tables: Table[],
	shape: types.Shape | undefined,
) => {
	const selectsPerTable: {
		selects: {
			from: string;
			as: string;
		}[];
		joins: Map<string, JoinOrRootTable>;
		groupBy: string | undefined;
	}[] = [];

	for (const table of tables) {
		const {
			selects,
			join: joinFromSelect,
			groupBy,
		} = selectAllFieldsFromTable(table, shape);

		selectsPerTable.push({ selects, joins: joinFromSelect, groupBy });
	}

	// pad with empty selects to make sure all selects have the same length
	let newSelects: {
		from: string;
		as: string;
	}[][] = [];

	for (const [i, selects] of selectsPerTable.entries()) {
		const newSelect = [];
		for (const [j, selectsOther] of selectsPerTable.entries()) {
			if (i !== j) {
				for (const select of selectsOther.selects) {
					newSelect.push({ from: "NULL", as: select.as });
				}
			} else {
				selects.selects.forEach((select) => newSelect.push(select));
			}
		}
		newSelects.push(newSelect);
	}
	// also return table name
	for (const [i, selects] of selectsPerTable.entries()) {
		selects.selects = newSelects[i];
	}

	return selectsPerTable;
};

export const selectAllFieldsFromTable = (
	table: Table,
	shape: types.Shape | undefined,
) => {
	let stack: { table: Table; shape?: types.Shape }[] = [{ table, shape }];
	let join: Map<string, JoinTable> = new Map();
	const fieldResolvers: { from: string; as: string }[] = [];
	let groupByParentId = false;
	for (const tableAndShape of stack) {
		if (tableAndShape.table.referencedInArray) {
			let selectBuilder = `${JSON_GROUP_ARRAY}(${JSON_OBJECT}(`;

			groupByParentId = true; // we need to group by the parent id as else we will not be returned with more than 1 result

			let first = false;
			const as = createReconstructReferenceName(tableAndShape.table);

			for (const field of tableAndShape.table.fields) {
				if (
					(field.isPrimary ||
						!tableAndShape.shape ||
						matchFieldInShape(tableAndShape.shape, [], field) ||
						// also always include the index field
						field.name === ARRAY_INDEX_COLUMN) &&
					field.name !== PARENT_TABLE_ID
				) {
					let resolveField = `${as}.${escapeColumnName(field.name)}`;
					// if field is bigint we need to convert it to string, so that later in a JSON.parse scenario it is not converted to a number, but remains a string until we can convert it back to a bigint manually
					if (field.unwrappedType === "u64") {
						resolveField = `CAST(${resolveField} AS TEXT)`;
					}

					// if field is blob we need to convert it to hex string
					if (field.type === "BLOB") {
						resolveField = `HEX(${resolveField})`;
					}

					if (first) {
						selectBuilder += `, `;
					}
					first = true;
					selectBuilder += `${escapeColumnName(field.name, "'")}, ${resolveField}`;
				}
			}
			selectBuilder += `))  `; // FILTER (WHERE ${tableAndShape.table.name}.${tableAndShape.table.primary} IS NOT NULL)

			fieldResolvers.push({
				from: selectBuilder,
				as,
			});

			join.set(createReconstructReferenceName(tableAndShape.table), {
				as,
				table: tableAndShape.table,
				type: "left" as const,
				columns: [],
			});
		} else if (!tableAndShape.table.inline) {
			// we end up here when we have simple joins we want to make that are not arrays, and not inlined
			if (tableAndShape.table.parent != null) {
				join.set(createReconstructReferenceName(tableAndShape.table), {
					as: tableAndShape.table.name,
					table: tableAndShape.table,
					type: "left" as const,
					columns: [],
				});
			}

			for (const field of tableAndShape.table.fields) {
				if (
					field.isPrimary ||
					!tableAndShape.shape ||
					matchFieldInShape(tableAndShape.shape, [], field)
				) {
					fieldResolvers.push({
						from: `${tableAndShape.table.name}.${escapeColumnName(field.name)}`,
						as: `'${getTablePrefixedField(tableAndShape.table, field.name)}'`,
					});
				}
			}
		}

		for (const child of tableAndShape.table.children) {
			let childShape: types.Shape | undefined = undefined;
			if (tableAndShape.shape) {
				const parentPath = child.parentPath?.slice(1);
				let maybeShape = parentPath
					? tableAndShape.shape?.[parentPath[parentPath.length - 1]]
					: undefined;

				if (!maybeShape) {
					continue;
				}

				childShape =
					maybeShape === true
						? undefined
						: Array.isArray(maybeShape)
							? maybeShape[0]
							: maybeShape;
			}
			stack.push({ table: child, shape: childShape });
		}
	}

	if (fieldResolvers.length === 0) {
		throw new Error("No fields to resolve");
	}

	return {
		groupBy: groupByParentId
			? `${table.name}.${escapeColumnName(table.primary as string)}` ||
				undefined
			: undefined,
		selects: fieldResolvers, //  `SELECT ${fieldResolvers.join(", ")} FROM ${table.name}`,
		join,
	};
};

const getNonInlinedTable = (from: Table) => {
	let current: Table = from;
	while (current.inline) {
		if (!current.parent) {
			throw new Error("No parent found");
		}
		current = current.parent;
	}
	return current;
};

// the inverse of resolveFieldValues
export const resolveInstanceFromValue = async <
	T,
	S extends types.Shape | undefined,
>(
	fromTablePrefixedValues: Record<string, any>,
	tables: Map<string, Table>,
	table: Table,
	resolveChildren: (parentId: any, table: Table) => Promise<any[]>,
	tablePrefixed: boolean,
	shape?: S,
): Promise<types.ReturnTypeFromShape<T, S>> => {
	const fields = getSchema(table.ctor).fields;
	const obj: any = {};

	const handleNested = async (
		field: Field,
		isOptional: boolean,
		isArray: boolean,
	) => {
		const subTables = getTableFromField(table, tables, field); // TODO fix

		let maybeShape = shape?.[field.key];
		const subshapeIsArray = Array.isArray(maybeShape);

		if (isArray && maybeShape && !subshapeIsArray && maybeShape !== true) {
			throw new Error(
				"Shape is not matching the array field type: " +
					field.key +
					". Shape: " +
					JSON.stringify(shape),
			);
		}

		let subshape =
			maybeShape === true
				? undefined
				: subshapeIsArray
					? maybeShape[0]
					: maybeShape;

		if (isArray) {
			/* let once = false; */
			let resolvedArr = [];

			for (const subtable of subTables) {
				// check if the array already in the provided row
				let arr: any[] | undefined = undefined;
				const tableName = createReconstructReferenceName(subtable);
				if (fromTablePrefixedValues[tableName]) {
					arr = JSON.parse(fromTablePrefixedValues[tableName]) as Array<any>;
					arr = arr.filter((x) => x[subtable.primary as string] != null);

					// we need to go over all fields that are to be bigints and convert
					// them back to bigints
					// for blob fields we need to convert them back to Uint8Array
					for (const field of subtable.fields) {
						if (field.name === PARENT_TABLE_ID) {
							continue;
						}
						if (field.unwrappedType === "u64") {
							for (const item of arr!) {
								item[field.name] = BigInt(item[field.name]);
							}
						} else if (field.type === "BLOB") {
							for (const item of arr!) {
								item[field.name] = fromHexString(item[field.name]);
							}
						}
					}
				} else {
					if (subtable.children) {
						// TODO we only end up where when we resolve nested arrays,
						// which shoulld instead be resolved in a nested select (with json_group_array and json_object)
						let rootTable = getNonInlinedTable(table);
						const parentId =
							fromTablePrefixedValues[
								getTablePrefixedField(
									rootTable,
									rootTable.primary as string,
									!tablePrefixed,
								)
							];

						arr = await resolveChildren(parentId, subtable);
					} else {
						arr = [];
					}
				}
				if (arr && arr.length > 0) {
					/* once = true; */
					for (const element of arr) {
						const resolved: SimpleNested | any = await resolveInstanceFromValue(
							element,
							tables,
							subtable, // TODO fix
							resolveChildren,
							false,
							subshape,
						);

						resolvedArr[element[ARRAY_INDEX_COLUMN]] = subtable.isSimpleValue
							? resolved.value
							: resolved;
					}
				}
			}

			obj[field.key] = resolvedArr; // we can not do option(vec('T')) since we dont store the option type for Arrays (TODO)
		} else {
			// resolve nested object from row directly
			/* let extracted: any = {} */
			let subTable: Table | undefined = undefined;
			if (subTables.length > 1) {
				for (const table of subTables) {
					// TODO types
					if (
						fromTablePrefixedValues[
							getTablePrefixedField(
								table,
								table.primary as string,
								!tablePrefixed,
							)
						] != null
					) {
						subTable = table;
						break;
					}
				}
			} else {
				subTable = subTables[0];
			}

			if (!subTable) {
				throw new Error("Sub table not found");
			}
			/* 
						for (const field of subTable.fields) {
							once = true
							extracted[field.name] = fromTablePrefixedValues[getTablePrefixedField(subTable, field.name, !tablePrefixed)]
						}
			 */

			if (subTable.inline && isOptional) {
				let rootTable = getNonInlinedTable(table);

				const isNull =
					!fromTablePrefixedValues[
						getTablePrefixedField(
							rootTable,
							subTable.fields[subTable.fields.length - 1].name,
						)
					];

				if (isNull) {
					obj[field.key] = undefined;
					return;
				}
			}

			// TODO types
			if (
				subTable.primary !== false &&
				fromTablePrefixedValues[
					getTablePrefixedField(subTable, subTable.primary, !tablePrefixed)
				] == null
			) {
				obj[field.key] = undefined;
			} else {
				const resolved = await resolveInstanceFromValue(
					fromTablePrefixedValues,
					tables,
					subTable,
					resolveChildren,
					tablePrefixed,
					subshape,
				);

				obj[field.key] = resolved;
			}
		}
	};

	for (const field of fields) {
		if (shape && !shape[field.key]) {
			continue;
		}

		const rootTable = getNonInlinedTable(table);
		const referencedField = rootTable.fields.find(
			(sqlField) => sqlField.from === field,
		);
		const fieldValue = referencedField
			? fromTablePrefixedValues[
					getTablePrefixedField(
						rootTable,
						referencedField!.name,
						!tablePrefixed,
					)
				]
			: undefined;
		if (typeof field.type === "string" || isUint8ArrayType(field.type)) {
			obj[field.key] = convertFromSQLType(fieldValue, field.type);
		} else if (field.type instanceof OptionKind) {
			if (
				typeof field.type.elementType === "string" ||
				isUint8ArrayType(field.type.elementType)
			) {
				obj[field.key] = convertFromSQLType(fieldValue, field.type.elementType);
			} else if (field.type.elementType instanceof VecKind) {
				await handleNested(field, true, true);
			} else {
				await handleNested(field, true, false);
			}
		} else if (field.type instanceof VecKind) {
			await handleNested(field, false, true);
		} else {
			await handleNested(field, false, false);
		}
	}

	return Object.assign(Object.create(table.ctor.prototype), obj);
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

export const convertDeleteRequestToQuery = (
	request: types.DeleteOptions,
	tables: Map<string, Table>,
	table: Table,
	options?: {
		planner?: PlanningSession;
	},
): { sql: string; bindable: any[] } => {
	const { query, bindable } = convertRequestToQuery(
		"delete",
		{ query: coerceLocalQueries(request.query) },
		tables,
		table,
		undefined,
		[],
		options,
	);
	return {
		sql: `DELETE FROM ${table.name} WHERE ${table.name}.${table.primary} IN (SELECT ${table.primary} from ${table.name} ${query}) returning ${table.primary}`,
		bindable,
	};
};

export const convertSumRequestToQuery = (
	request: types.SumOptions,
	tables: Map<string, Table>,
	table: Table,
	options?: {
		planner?: PlanningSession;
	},
): { sql: string; bindable: any[] } => {
	const { query, bindable } = convertRequestToQuery(
		"sum",
		{ query: coerceLocalQueries(request.query), key: request.key },
		tables,
		table,
		undefined,
		[],
		options,
	);

	const inlineName = getInlineTableFieldName(request.key);
	const field = table.fields.find((x) => x.name === inlineName);
	if (unwrapNestedType(field!.from!.type) === "u64") {
		throw new Error("Summing is not supported for u64 fields");
	}
	const column = `${table.name}.${getInlineTableFieldName(request.key)}`;

	return {
		sql: `SELECT SUM(${column}) as sum FROM ${table.name} ${query}`,
		bindable,
	};
};

export const convertCountRequestToQuery = (
	request: types.CountOptions | undefined,
	tables: Map<string, Table>,
	table: Table,
): { sql: string; bindable: any[] } => {
	const { query, bindable } = convertRequestToQuery(
		"count",
		{ query: coerceLocalQueries(request?.query) },
		tables,
		table,
	);
	return {
		sql: `SELECT count(DISTINCT ${table.name}.${table.primary!}) as count FROM ${table.name} ${query}`,
		bindable,
	};
};

/** One term of a search's ORDER BY, as `buildOrderBy` resolved it. */
type OrderByTerm = {
	/** The expression the term sorts by (its resolver select). */
	from: string;
	direction: types.SortDirection;
	/** The column `from` names. */
	column: string;
	foreignTable: JoinOrRootTable;
};

const buildOrderBy = (
	sort: types.Sort[] | types.Sort | undefined,
	tables: Map<string, Table>,
	table: Table,
	joinBuilder: Map<string, JoinOrRootTable>,
	resolverBuilder: { from: string; as: string }[],
	path: string[] = [],
	options?: {
		fetchAll?: boolean;
		planner?: PlanningSession;
	},
) => {
	let orderByBuilder: string | undefined = undefined;
	const terms: OrderByTerm[] = [];

	if (
		(!sort || (Array.isArray(sort) && sort.length === 0)) &&
		!options?.fetchAll
	) {
		sort =
			table.primary && path.length === 0
				? [{ key: [table.primary], direction: types.SortDirection.ASC }]
				: undefined;
	}

	if (sort) {
		let sortArr = Array.isArray(sort) ? sort : [sort];
		if (sortArr.length > 0) {
			orderByBuilder = "";
			let once = false;
			for (const sort of sortArr) {
				const { foreignTables, queryKey } = resolveTableToQuery(
					table,
					tables,
					joinBuilder,
					[...path, ...sort.key],
					undefined,
					true,
				);

				for (const foreignTable of foreignTables) {
					if (once) {
						orderByBuilder += ", ";
					}
					once = true;

					foreignTable.columns.push(queryKey); // add the sort key to the list of columns that will be used for this query
					orderByBuilder += `"${foreignTable.as}#${queryKey}" ${sort.direction === types.SortDirection.ASC ? "ASC" : "DESC"}`;

					const from = `${table.name}.${escapeColumnName(queryKey)}`;
					resolverBuilder.push({
						from,
						as: `'${foreignTable.as}#${queryKey}'`,
					});
					terms.push({
						from,
						direction: sort.direction,
						column: queryKey,
						foreignTable,
					});
				}
			}
		}
	}

	return { orderByBuilder, terms };
};

export const convertSearchRequestToQuery = (
	request:
		| { query: types.Query[]; sort?: types.Sort[] | types.Sort }
		| undefined,
	tables: Map<string, Table>,
	rootTables: Table[],
	options?: {
		shape?: types.Shape | undefined;
		fetchAll?: boolean;
		planner?: PlanningSession;
	},
): { sql: string; bindable: any[] } => {
	const normalizedRequest = request
		? {
				query: coerceLocalQueries(request.query),
				sort: coerceLocalSorts(request.sort),
			}
		: undefined;
	let unionBuilder = "";
	let orderByClause: string = "";

	let matchedOnce = false;
	let lastError: Error | undefined = undefined;

	const selectsPerTable = selectAllFieldsFromTables(rootTables, options?.shape);
	let bindableBuilder: any[] = [];

	for (const [i, table] of rootTables.entries()) {
		const { selects, joins, groupBy } = selectsPerTable[i];
		let tableOrderByClause: string;

		try {
			const { orderByBuilder } = buildOrderBy(
				normalizedRequest?.sort,
				tables,
				table,
				joins,
				selects,
				[],
				options,
			);

			tableOrderByClause = orderByBuilder ?? "";
		} catch (error) {
			if (error instanceof MissingFieldError) {
				lastError = error;
				continue;
			}
			throw error;
		}

		const selectQuery = generateSelectQuery(table, selects);

		for (const flattenRequest of flattenQuery(normalizedRequest)) {
			try {
				const { query, bindable } = convertRequestToQuery(
					"iterate",
					flattenRequest,
					tables,
					table,
					new Map(joins), // copy the map, else we might might do unececessary joins
					[],
					options,
				);

				unionBuilder += `${unionBuilder.length > 0 ? " UNION " : ""} ${selectQuery} ${query} ${groupBy ? "GROUP BY " + groupBy : ""}`;
				if (!matchedOnce) {
					// Use an alias from a participating SELECT, never a skipped root.
					orderByClause = tableOrderByClause;
				}
				matchedOnce = true;
				bindableBuilder.push(...bindable);
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					continue;
				}
				throw error;
			}
		}
	}

	if (!matchedOnce) {
		throw lastError!;
	}

	return {
		sql: `${unionBuilder} ${orderByClause ? "ORDER BY " + orderByClause : ""} ${options?.fetchAll ? "" : "limit ? offset ?"}`,
		bindable: bindableBuilder,
	};
};

/*
 * Key statements: they read a search in its order key K and bind no OFFSET.
 *
 * K = (sort terms..., primary key[, root ordinal]):
 * - the primary key breaks ties, in the direction of the last sort term (ASC
 *   without a sort). A sort term on the primary key ends K: the key is unique.
 * - With several root tables, the root table's ordinal comes last (ASC).
 *
 * Every key statement selects K under positional aliases, which are the same
 * in every UNION arm (`__s0`, `__s1`, ... for the sort terms, `__pk` and
 * `__t`), and orders by all of K. SQLite decides every order; a caller compares
 * keys only for identity.
 *
 * - S1, head page (`convertSearchRequestToPageQuery`): the search, hydrated,
 *   `ORDER BY K LIMIT ?`.
 * - S2, continuation (the same, with `after`): S1 limited to the rows after a
 *   cursor (`buildKeysetPredicate`), one statement per NULL pattern of the
 *   cursor.
 * - S3, key list (`convertSearchRequestToKeyQuery`): `__t`, `__pk` and the sort
 *   terms of every matching row in K order, without hydration.
 * - S4, re-check (the same, with `restrictToKeys`): S3 for the keys of a JSON
 *   key list, read through the primary key. With `after` it also selects
 *   `__after`.
 * - S5, rows by key (`convertKeysToRowsQuery`): the hydrated rows of one root
 *   table for a JSON key list, in no order.
 *
 * Values known only when a statement runs (the cursor, the key list, the
 * limit) are `KeyQueryParameter`s in `bindable`; `bindKeyQuery` binds them.
 *
 * With several root tables, a statement that compares a cursor (S2, `__after`)
 * or one shared key list (S4) with a term that the tables store differently
 * throws a KeyTypeMismatchError; S4 with a key list per root table
 * (`keysPerRoot`) does not. S1, S3 and S5 compare no bound value across
 * tables. The builders also return their arms (`KeyStatementArms`).
 */

export const ROOT_ORDINAL_ALIAS = "__t";
export const PRIMARY_KEY_ALIAS = "__pk";
export const AFTER_CURSOR_ALIAS = "__after";
export const sortTermAlias = (index: number) => `__s${index}`;

/**
 * The storage class of a key term's non-NULL values in a root table (the
 * tables are STRICT): `numeric` for INTEGER and REAL, which compare as
 * numbers, `text` for TEXT and `blob` for BLOB.
 */
export type KeyTermType = "numeric" | "text" | "blob";

export type OrderKeyTerm = {
	/** The term's result alias in every key statement. */
	alias: string;
	direction: types.SortDirection;
	/** Whether the term's column can hold NULL (an optional field). */
	nullable: boolean;
	/**
	 * The storage class of the term's values, the same in every root table the
	 * statement reads; undefined when the tables differ or one is unknown.
	 */
	type: KeyTermType | undefined;
};

export type OrderKey = {
	/** The sort terms, `__pk`, then `__t` when there are several root tables. */
	terms: OrderKeyTerm[];
	/** Whether the request names a sort (if not, K is the primary key). */
	sorted: boolean;
};

/**
 * A key statement would compare a cursor or a key list across root tables
 * that store a term of K differently.
 *
 * SQLite converts a bound value to a column's type before comparing it (TEXT
 * '10' becomes 10 against an INTEGER column, 5 becomes '5' against a TEXT
 * column), while ORDER BY compares storage classes (numbers before TEXT, TEXT
 * before BLOB) and converts nothing. So:
 * - a cursor (`after`) cannot be compared across root tables where a term of
 *   K holds numbers in one and TEXT in another: the statement would skip or
 *   repeat rows of S3's order;
 * - a key list (`restrictToKeys`) cannot be matched across root tables whose
 *   primary keys are stored differently (another storage class, or a u64 key
 *   shifted into the signed range): a key of one table would match a
 *   different key of another.
 *
 * Statements over one root table never throw it.
 */
export class KeyTypeMismatchError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "KeyTypeMismatchError";
	}
}

/** The storage class of a column of SQL type `sqlType` (as `toSQLType` names it). */
const keyTermType = (sqlType: string | undefined): KeyTermType | undefined => {
	const name = /^\s*(INTEGER|REAL|TEXT|BLOB)\b/i.exec(sqlType ?? "")?.[1];
	if (!name) {
		return undefined;
	}
	const upper = name.toUpperCase();
	return upper === "TEXT" ? "text" : upper === "BLOB" ? "blob" : "numeric";
};

/**
 * How a root table stores its primary keys: its storage class, and whether a
 * u64 key is shifted into the signed range (`convertToSQLType`). Root tables
 * with the same encoding store one key as the same SQL value.
 */
export const primaryKeyEncoding = (table: Table): string | undefined => {
	const type = keyTermType(table.primaryField?.type);
	return type && table.primaryField?.from?.type === "u64"
		? `${type} (u64)`
		: type;
};

/** A parameter of a key statement, bound by `bindKeyQuery`. */
export class KeyQueryParameter {
	private static readonly cursorTerms: KeyQueryParameter[] = [];
	private static readonly rootKeyLists: KeyQueryParameter[] = [];
	static readonly keys = new KeyQueryParameter("keys", -1);
	static readonly limit = new KeyQueryParameter("limit", -1);
	/** The cursor's value of K's term `term`. */
	static cursor(term: number): KeyQueryParameter {
		return (KeyQueryParameter.cursorTerms[term] ??= new KeyQueryParameter(
			"cursor",
			term,
		));
	}
	/**
	 * The key list of the root table with ordinal `ordinal` (`term` holds the
	 * ordinal), for a re-check built with `keysPerRoot`.
	 */
	static rootKeys(ordinal: number): KeyQueryParameter {
		return (KeyQueryParameter.rootKeyLists[ordinal] ??= new KeyQueryParameter(
			"rootKeys",
			ordinal,
		));
	}
	private constructor(
		readonly kind: "cursor" | "keys" | "rootKeys" | "limit",
		readonly term: number,
	) {}
}

/**
 * The values to run a key statement with: `KeyQueryParameter`s replaced by
 * `cursor` (the K values of the last row read, raw SQL values in term order),
 * `keys` (from `encodeKeyList`), `rootKeys` (one such list per root ordinal,
 * each in its table's encoding) and `limit` (Infinity reads to the end).
 */
export const bindKeyQuery = (
	bindable: readonly unknown[],
	values: {
		cursor?: readonly unknown[];
		keys?: string;
		rootKeys?: readonly (string | undefined)[];
		limit?: number | bigint;
	},
): BindableValue[] =>
	bindable.map((value) => {
		if (!(value instanceof KeyQueryParameter)) {
			return value as BindableValue;
		}
		if (value.kind === "cursor") {
			const cursorValue = values.cursor?.[value.term];
			if (cursorValue == null) {
				throw new Error(`Missing a cursor value for key term ${value.term}`);
			}
			return cursorValue as BindableValue;
		}
		if (value.kind === "keys") {
			if (typeof values.keys !== "string") {
				throw new Error("Missing the key list");
			}
			return values.keys;
		}
		if (value.kind === "rootKeys") {
			const keys = values.rootKeys?.[value.term];
			if (typeof keys !== "string") {
				throw new Error(
					`Missing the key list of the root table with ordinal ${value.term}`,
				);
			}
			return keys;
		}
		const limit = values.limit;
		if (limit === Infinity) {
			return -1; // SQLite reads to the end with a negative LIMIT
		}
		if (
			typeof limit !== "bigint" &&
			(typeof limit !== "number" || !Number.isInteger(limit))
		) {
			throw new Error(`Invalid limit: ${String(limit)}`);
		}
		return limit;
	});

/**
 * The rows strictly after a cursor in the order (`columns`, `directions`),
 * with SQLite's NULL order (NULL is smallest: first in ASC, last in DESC).
 * `nulls` is the cursor's NULL pattern. `binds` lists, in textual order, the
 * cursor term each `?` takes; a NULL cursor term binds nothing.
 *
 * With one direction this is a row value, `(c1, ..., cn) > (?, ..., ?)`, when
 * no column can be NULL, or when the direction is ASC and the cursor has no
 * NULL: a row value is NULL (false) once its decision reaches a NULL in the
 * row, and such a row sorts before the cursor in ASC. Otherwise it is the
 * expansion `c1 > ? OR (c1 = ? AND (c2 > ? OR ...))`, led by a range on the
 * first column when that range keeps every row after the cursor, so SQLite
 * can still seek.
 */
export const buildKeysetPredicate = (
	columns: string[],
	directions: types.SortDirection[],
	nullable: boolean[],
	nulls: boolean[],
): { sql: string; binds: number[] } => {
	const n = columns.length;
	if (
		n === 0 ||
		directions.length !== n ||
		nullable.length !== n ||
		nulls.length !== n
	) {
		throw new Error(
			"A keyset predicate needs one direction, nullability and NULL flag per column",
		);
	}
	for (let i = 0; i < n; i++) {
		if (nulls[i] && !nullable[i]) {
			throw new Error(`Key term ${i} cannot be NULL`);
		}
	}
	const ascending = (i: number) => directions[i] === types.SortDirection.ASC;
	if (
		directions.every((direction) => direction === directions[0]) &&
		(!nullable.some(Boolean) || (ascending(0) && !nulls.some(Boolean)))
	) {
		const operator = ascending(0) ? ">" : "<";
		return n === 1
			? { sql: `${columns[0]} ${operator} ?`, binds: [0] }
			: {
					sql: `(${columns.join(", ")}) ${operator} (${columns.map(() => "?").join(", ")})`,
					binds: columns.map((_, i) => i),
				};
	}
	const binds: number[] = [];
	const greater = (i: number): string => {
		if (ascending(i)) {
			if (nulls[i]) {
				return `${columns[i]} IS NOT NULL`;
			}
			binds.push(i);
			return `${columns[i]} > ?`;
		}
		if (nulls[i]) {
			return "0"; // NULL is last in DESC: nothing comes after it
		}
		binds.push(i);
		return nullable[i]
			? `(${columns[i]} < ? OR ${columns[i]} IS NULL)`
			: `${columns[i]} < ?`;
	};
	const equal = (i: number): string => {
		if (nulls[i]) {
			return `${columns[i]} IS NULL`;
		}
		binds.push(i);
		return `${columns[i]} = ?`;
	};
	const after = (i: number): string => {
		if (i === n - 1) {
			return greater(i);
		}
		const greaterSql = greater(i);
		const equalSql = equal(i);
		return `(${greaterSql} OR (${equalSql} AND ${after(i + 1)}))`;
	};
	let range = "";
	if (!nulls[0]) {
		if (ascending(0)) {
			range = `${columns[0]} >= ? AND `;
		} else if (!nullable[0]) {
			range = `${columns[0]} <= ? AND `;
		}
	}
	const body = after(0);
	return { sql: `${range}${body}`, binds: range ? [0, ...binds] : binds };
};

const SORT_DIRECTION_SQL = (direction: types.SortDirection) =>
	direction === types.SortDirection.DESC ? "DESC" : "ASC";

const orderByKey = (order: OrderKey) =>
	order.terms
		.map((term) => `"${term.alias}" ${SORT_DIRECTION_SQL(term.direction)}`)
		.join(", ");

const asSortArray = (sort: types.Sort[] | types.Sort | undefined) =>
	sort ? (Array.isArray(sort) ? sort : [sort]) : [];

/**
 * K for one root table: the expression of each term in this table's arms, and
 * the terms. Throws a MissingFieldError when the table lacks a sort field.
 */
const resolveOrderKey = (
	sorts: types.Sort[],
	tables: Map<string, Table>,
	table: Table,
	ordinal: number,
	severalRoots: boolean,
	joins: Map<string, JoinOrRootTable>,
	planner: PlanningSession | undefined,
): { expressions: string[]; terms: OrderKeyTerm[] } => {
	if (table.primary === false) {
		throw new Error("Key statements need a primary key");
	}
	const { terms: sortTerms } = buildOrderBy(
		sorts,
		tables,
		table,
		joins,
		[], // positional aliases replace the resolver selects
		[],
		{ planner },
	);
	const primaryKey = `${table.name}.${escapeColumnName(table.primary)}`;
	const expressions: string[] = [];
	const terms: OrderKeyTerm[] = [];
	let primaryKeyDirection: types.SortDirection | undefined;
	for (const term of sortTerms) {
		const rootColumn = term.foreignTable.type === "root";
		if (rootColumn && term.column === table.primary) {
			primaryKeyDirection = term.direction;
			break;
		}
		// Scalar columns are created without NOT NULL, and a field written as
		// undefined (or an inline object written as null) is stored as NULL,
		// whatever its schema type. So every sort column can hold NULL unless it
		// is declared NOT NULL.
		const field = rootColumn
			? table.fields.find((candidate) => candidate.name === term.column)
			: undefined;
		expressions.push(term.from);
		terms.push({
			alias: sortTermAlias(terms.length),
			direction: term.direction,
			nullable: !field || !/\bNOT NULL\b/i.test(field.type),
			type: keyTermType(
				term.foreignTable.table.fields.find(
					(candidate) => candidate.name === term.column,
				)?.type,
			),
		});
	}
	expressions.push(primaryKey);
	terms.push({
		alias: PRIMARY_KEY_ALIAS,
		direction:
			primaryKeyDirection ??
			(sorts.length > 0
				? sorts[sorts.length - 1].direction
				: types.SortDirection.ASC),
		nullable: false,
		type: keyTermType(table.primaryField?.type),
	});
	if (severalRoots) {
		expressions.push(String(ordinal));
		terms.push({
			alias: ROOT_ORDINAL_ALIAS,
			direction: types.SortDirection.ASC,
			nullable: false,
			type: "numeric",
		});
	}
	return { expressions, terms };
};

type KeyArm = {
	/** The root table's ordinal: its index in `rootTables`, the value of `__t`. */
	ordinal: number;
	table: Table;
	/** The expression of each term of K in this arm. */
	key: string[];
};

/**
 * A key statement's arms, which a caller needs (with `order`) to pick how to
 * read a search.
 */
export type KeyStatementArms = {
	/**
	 * The statement's UNION arms: one per root table and flattened branch,
	 * without the skipped ones.
	 */
	arms: number;
	/**
	 * Whether an arm groups its rows by the primary key: its query joins a
	 * child table, or (S1, S2, S5) its hydration aggregates child rows.
	 */
	grouped: boolean;
};

/**
 * The UNION of a search's arms (root table x flattened branch), with
 * `convertSearchRequestToQuery`'s MissingFieldError skip: a root table that
 * lacks a sort field, and a branch that names a missing field, add no arm. If
 * no arm remains, the last MissingFieldError is thrown. Each arm selects
 * `selects` (hydration, for S1 and S2), then K, then `arm(...).selects`; a
 * condition from `arm(...).where` is AND-ed after its query. An arm whose
 * query joins a child table, or whose selects aggregate, groups by the
 * primary key.
 *
 * `compares` names what the arms compare with bound values: a cursor with K,
 * or a key list with the primary key. Across root tables that store the
 * compared terms differently that throws a KeyTypeMismatchError.
 */
const buildKeyUnion = (
	request:
		| { query: types.Query[]; sort?: types.Sort[] | types.Sort }
		| undefined,
	tables: Map<string, Table>,
	rootTables: Table[],
	options: {
		planner?: PlanningSession;
		hydrate?: { shape: types.Shape | undefined };
		primaryKeyAccess?: boolean;
		compares?: { cursor?: boolean; keys?: boolean };
		arm?: (
			arm: KeyArm,
			order: OrderKey,
		) => {
			selects?: { from: string; as: string }[];
			/** Parameters of `selects`; they bind before the arm's query. */
			selectBindable?: unknown[];
			where?: { sql: string; bindable: unknown[] };
		};
	},
): {
	union: string;
	bindable: unknown[];
	order: OrderKey;
} & KeyStatementArms => {
	const normalizedRequest = request
		? {
				query: coerceLocalQueries(request.query),
				sort: coerceLocalSorts(request.sort),
			}
		: undefined;
	const sorts = asSortArray(normalizedRequest?.sort);
	const hydration = options.hydrate
		? selectAllFieldsFromTables(rootTables, options.hydrate.shape)
		: undefined;
	let lastError: Error | undefined = undefined;

	// K per root table. Its terms must agree across tables; a term is nullable
	// when any table can store NULL in it.
	const roots: {
		ordinal: number;
		table: Table;
		joins: Map<string, JoinOrRootTable>;
		/** Joins present before the query: hydration's own. */
		baseJoins: Set<string>;
		expressions: string[];
		/** The storage class of each term of K in this table. */
		types: (KeyTermType | undefined)[];
		/** The arms this table adds. */
		arms: number;
	}[] = [];
	let order: OrderKey | undefined = undefined;
	for (const [ordinal, table] of rootTables.entries()) {
		const joins = hydration
			? hydration[ordinal].joins
			: new Map<string, JoinOrRootTable>();
		const baseJoins = new Set(joins.keys());
		let resolved: { expressions: string[]; terms: OrderKeyTerm[] };
		try {
			resolved = resolveOrderKey(
				sorts,
				tables,
				table,
				ordinal,
				rootTables.length > 1,
				joins,
				options.planner,
			);
		} catch (error) {
			if (error instanceof MissingFieldError) {
				lastError = error;
				continue;
			}
			throw error;
		}
		if (!order) {
			order = { terms: resolved.terms, sorted: sorts.length > 0 };
		} else {
			if (
				resolved.terms.length !== order.terms.length ||
				resolved.terms.some(
					(term, i) =>
						term.alias !== order!.terms[i].alias ||
						term.direction !== order!.terms[i].direction,
				)
			) {
				throw new Error(
					"The sort resolves to different key terms in different root tables",
				);
			}
			order.terms = order.terms.map((term, i) =>
				resolved.terms[i].nullable && !term.nullable
					? { ...term, nullable: true }
					: term,
			);
		}
		roots.push({
			ordinal,
			table,
			joins,
			baseJoins,
			expressions: resolved.expressions,
			types: resolved.terms.map((term) => term.type),
			arms: 0,
		});
	}

	let union = "";
	const bindable: unknown[] = [];
	let matchedOnce = false;
	let grouped = false;
	for (const root of roots) {
		const { ordinal, table, joins, baseJoins, expressions } = root;
		const primaryKey = `${table.name}.${escapeColumnName(table.primary as string)}`;
		const keySelects = order!.terms.map((term, i) => ({
			from: expressions[i],
			as: `"${term.alias}"`,
		}));
		for (const flattenRequest of flattenQuery(normalizedRequest)) {
			try {
				const extra = options.arm?.(
					{ ordinal, table, key: expressions },
					order!,
				);
				const branchJoins = new Map(joins); // copy the map, else we might do unnecessary joins
				const { query, bindable: queryBindable } = convertRequestToQuery(
					"iterate",
					flattenRequest,
					tables,
					table,
					branchJoins,
					[],
					{
						planner: options.planner,
						extraWhere: extra?.where,
						primaryKeyAccess: options.primaryKeyAccess,
					},
				);
				const queryJoinsChildTable = [...branchJoins].some(
					([name, join]) => join.type !== "root" && !baseJoins.has(name),
				);
				const groupBy =
					hydration?.[ordinal].groupBy ??
					(queryJoinsChildTable ? primaryKey : undefined);
				const selects = [
					...(hydration ? hydration[ordinal].selects : []),
					...keySelects,
					...(extra?.selects ?? []),
				];
				union += `${union.length > 0 ? " UNION " : ""}${generateSelectQuery(table, selects)} ${query}${groupBy ? " GROUP BY " + groupBy : ""}`;
				bindable.push(...(extra?.selectBindable ?? []), ...queryBindable);
				root.arms++;
				grouped ||= !!groupBy;
				matchedOnce = true;
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					continue;
				}
				throw error;
			}
		}
	}

	if (!matchedOnce) {
		throw lastError!;
	}

	// The storage class of each term across the root tables that add arms.
	const read = roots.filter((root) => root.arms > 0);
	order!.terms = order!.terms.map((term, i) => ({
		...term,
		type: read.every(
			(root) =>
				root.types[i] !== undefined && root.types[i] === read[0].types[i],
		)
			? read[0].types[i]
			: undefined,
	}));
	if (read.length > 1) {
		const describe = (value: (root: (typeof read)[number]) => unknown) =>
			read
				.map((root) => `${root.table.name}: ${value(root) ?? "unknown"}`)
				.join(", ");
		if (options.compares?.cursor) {
			// Numbers and TEXT are the only storage classes SQLite converts
			// between when it compares a column with a bound value.
			order!.terms.forEach((term, i) => {
				const classes = new Set(read.map((root) => root.types[i]));
				if (
					classes.has(undefined) ||
					(classes.has("numeric") && classes.has("text"))
				) {
					throw new KeyTypeMismatchError(
						`Key term "${term.alias}" has values of different types in different root tables (${describe((root) => root.types[i])}): a cursor cannot be compared across them`,
					);
				}
			});
		}
		if (options.compares?.keys) {
			const encodings = new Set(
				read.map((root) => primaryKeyEncoding(root.table)),
			);
			if (encodings.size > 1 || encodings.has(undefined)) {
				throw new KeyTypeMismatchError(
					`The primary key is stored differently in different root tables (${describe((root) => primaryKeyEncoding(root.table))}): a key list cannot be matched across them`,
				);
			}
		}
	}
	return {
		union,
		bindable,
		order: order!,
		arms: read.reduce((sum, root) => sum + root.arms, 0),
		grouped,
	};
};

/** `buildKeysetPredicate` over one arm's K, with cursor parameters. */
const keysetPredicateForArm = (
	arm: KeyArm,
	order: OrderKey,
	nulls: boolean[],
): { sql: string; bindable: KeyQueryParameter[] } => {
	if (nulls.length !== order.terms.length) {
		throw new Error(
			`The cursor's NULL pattern has ${nulls.length} terms, the order key ${order.terms.length}`,
		);
	}
	const { sql, binds } = buildKeysetPredicate(
		arm.key,
		order.terms.map((term) => term.direction),
		order.terms.map((term) => term.nullable),
		nulls,
	);
	return {
		sql,
		bindable: binds.map((term) => KeyQueryParameter.cursor(term)),
	};
};

/**
 * S1 and S2: hydrated rows of a search in K order, `LIMIT ?`. Without `after`
 * this is the head page (S1). With `after`, the NULL pattern of the cursor
 * (one flag per term of K), it is the continuation (S2): the rows strictly
 * after the cursor. Rows carry K under its positional aliases, so the last row
 * of a page is the next cursor.
 *
 * S2 throws a KeyTypeMismatchError when its root tables store a term of K as
 * numbers in one and as TEXT in another.
 */
export const convertSearchRequestToPageQuery = (
	request:
		| { query: types.Query[]; sort?: types.Sort[] | types.Sort }
		| undefined,
	tables: Map<string, Table>,
	rootTables: Table[],
	options?: {
		shape?: types.Shape | undefined;
		planner?: PlanningSession;
		after?: boolean[];
	},
): { sql: string; bindable: unknown[]; order: OrderKey } & KeyStatementArms => {
	const after = options?.after;
	const { union, bindable, order, arms, grouped } = buildKeyUnion(
		request,
		tables,
		rootTables,
		{
			planner: options?.planner,
			hydrate: { shape: options?.shape },
			compares: { cursor: after != null },
			arm: after
				? (arm, order) => ({ where: keysetPredicateForArm(arm, order, after) })
				: undefined,
		},
	);
	return {
		sql: `${union} ORDER BY ${orderByKey(order)} LIMIT ?`,
		bindable: [...bindable, KeyQueryParameter.limit],
		order,
		arms,
		grouped,
	};
};

/**
 * S3 and S4: `__t`, `__pk` and the sort terms of a search's matching rows, in
 * K order, without hydration.
 *
 * - `restrictToKeys` (S4): only the rows whose primary key is in a JSON key
 *   list (`encodeKeyList`), reached through the primary key.
 * - `keysPerRoot`, with `restrictToKeys`: each root table's arms take their
 *   own key list (`KeyQueryParameter.rootKeys(ordinal)`), in the way that
 *   table stores its keys, so the re-check stays one statement across root
 *   tables that store the primary key differently.
 * - `primaryKeyAccess`: the planner forces no index on the root table. Always
 *   on with `restrictToKeys`: an index forced for the sort would scan every row
 *   to find a few keys.
 * - `after`, the NULL pattern of a cursor: each row also selects `__after`, 1
 *   when the row comes after the cursor in K order and 0 otherwise.
 *
 * Across root tables that store a compared term differently it throws a
 * KeyTypeMismatchError: with `after`, a term of K that is a number in one and
 * TEXT in another; with `restrictToKeys` and one shared key list, primary keys
 * stored differently.
 */
export const convertSearchRequestToKeyQuery = (
	request:
		| { query: types.Query[]; sort?: types.Sort[] | types.Sort }
		| undefined,
	tables: Map<string, Table>,
	rootTables: Table[],
	options?: {
		planner?: PlanningSession;
		restrictToKeys?: boolean;
		keysPerRoot?: boolean;
		primaryKeyAccess?: boolean;
		after?: boolean[];
	},
): { sql: string; bindable: unknown[]; order: OrderKey } & KeyStatementArms => {
	const after = options?.after;
	const restrictToKeys = options?.restrictToKeys === true;
	const keysPerRoot = restrictToKeys && options?.keysPerRoot === true;
	const { union, bindable, order, arms, grouped } = buildKeyUnion(
		request,
		tables,
		rootTables,
		{
			planner: options?.planner,
			primaryKeyAccess: restrictToKeys || options?.primaryKeyAccess === true,
			compares: {
				cursor: after != null,
				keys: restrictToKeys && !keysPerRoot,
			},
			arm: (arm, order) => {
				const flag = after
					? keysetPredicateForArm(arm, order, after)
					: undefined;
				return {
					selects: flag
						? [
								{
									from: `CASE WHEN ${flag.sql} THEN 1 ELSE 0 END`,
									as: `"${AFTER_CURSOR_ALIAS}"`,
								},
							]
						: undefined,
					selectBindable: flag?.bindable,
					where: restrictToKeys
						? {
								sql: `${arm.table.name}.${escapeColumnName(arm.table.primary as string)} IN (${keyListSubquery(arm.table)})`,
								bindable: [
									keysPerRoot
										? KeyQueryParameter.rootKeys(arm.ordinal)
										: KeyQueryParameter.keys,
								],
							}
						: undefined,
				};
			},
		},
	);
	return {
		sql: `${union} ORDER BY ${orderByKey(order)}`,
		bindable,
		order,
		arms,
		grouped,
	};
};

/**
 * S5: hydrated rows of one root table for a JSON key list (`encodeKeyList`),
 * in no particular order, with the primary key also as `__pk`. The primary key
 * is the access path: the root table gets no planner index, and child tables
 * are joined as in every other read. `grouped` says whether the hydration
 * aggregates child rows (GROUP BY the primary key).
 */
export const convertKeysToRowsQuery = (
	table: Table,
	shape: types.Shape | undefined,
	options?: { planner?: PlanningSession },
): { sql: string; bindable: unknown[]; grouped: boolean } => {
	if (table.primary === false) {
		throw new Error("Key statements need a primary key");
	}
	const { selects, join, groupBy } = selectAllFieldsFromTable(table, shape);
	const { join: joinSql } = buildJoin(join, {
		planner: options?.planner,
		primaryKeyTable: table,
	});
	const primaryKey = `${table.name}.${escapeColumnName(table.primary)}`;
	return {
		sql: `${generateSelectQuery(table, [
			...selects,
			{ from: primaryKey, as: `"${PRIMARY_KEY_ALIAS}"` },
		])} ${joinSql} WHERE ${primaryKey} IN (${keyListSubquery(table)})${groupBy ? " GROUP BY " + groupBy : ""}`,
		bindable: [KeyQueryParameter.keys],
		grouped: !!groupBy,
	};
};

/**
 * The keys of a JSON key list (`encodeKeyList`) as a subquery, in the SQL type
 * of the table's primary key: BLOB keys travel as hex strings.
 */
export const keyListSubquery = (table: Table) =>
	/^BLOB\b/i.test(table.primaryField?.type ?? "")
		? "SELECT unhex(value) FROM json_each(?)"
		: "SELECT value FROM json_each(?)";

const MIN_I64 = -(1n << 63n);
const MAX_I64 = (1n << 63n) - 1n;
const HEX_BYTES = Array.from({ length: 256 }, (_, i) =>
	i.toString(16).padStart(2, "0"),
);

// In JSON.stringify's output: an escaped backslash, matched whole so that its
// second backslash does not start an escape, or the escape of a lone surrogate.
const LONE_SURROGATE_ESCAPES = /\\\\|\\u(d[89a-f][0-9a-f]{2})/g;

/**
 * `value` as a JSON string in which each lone surrogate stays a raw code unit,
 * where JSON.stringify writes an escape (`\ud800`). The key list is bound as a
 * string, so the binding then encodes a lone surrogate just as it does when it
 * binds the key itself, and json_each returns those bytes as they are: U+FFFD,
 * as the WASM engine binds a lone surrogate and the Node driver makes
 * better-sqlite3 bind it on every Node version. json_each would decode an
 * escape to the surrogate's own 3 bytes (ED A0 80 for U+D800), which no
 * binding stores.
 */
const jsonString = (value: string) => {
	const json = JSON.stringify(value);
	// Every escape of a lone surrogate starts with `\ud`; most keys have none.
	return json.includes("\\ud")
		? json.replace(LONE_SURROGATE_ESCAPES, (escape, unit) =>
				unit ? String.fromCharCode(parseInt(unit, 16)) : escape,
			)
		: json;
};

/**
 * SQL key values (as stored, so a u64 key is already shifted into the signed
 * range) as one JSON array for `keyListSubquery`.
 *
 * - Integers become JSON integers, which SQLite reads back as exact 64-bit
 *   INTEGERs: a bigint and a number of equal value encode the same.
 * - Strings stay JSON strings, with each lone surrogate left raw for the
 *   binding to encode as it encodes the key (`jsonString`); bytes become hex
 *   strings.
 *
 * A string key finds the row it was written with, and a key read from a row
 * (S3's `__pk`, say) finds that row: every binding stores a lone surrogate as
 * U+FFFD. A key stored as other bytes that are not UTF-8 (a surrogate's own 3
 * bytes, as better-sqlite3 wrote them on Node 22 before the Node driver bound
 * strings well formed, or raw SQL) reads back with U+FFFD in their place and
 * binds to other bytes: like `WHERE id = ?`, its list finds no row, or another
 * key's row.
 */
export const encodeKeyList = (keys: Iterable<unknown>): string => {
	let out = "[";
	let first = true;
	for (const key of keys) {
		if (!first) {
			out += ",";
		}
		first = false;
		if (typeof key === "string") {
			out += jsonString(key);
		} else if (
			typeof key === "bigint" ||
			(typeof key === "number" && Number.isInteger(key))
		) {
			const integer = BigInt(key);
			if (integer < MIN_I64 || integer > MAX_I64) {
				throw new RangeError(
					`Key ${integer} is outside the 64-bit range SQLite stores`,
				);
			}
			out += integer.toString();
		} else if (typeof key === "number" && Number.isFinite(key)) {
			out += String(key);
		} else if (key instanceof Uint8Array) {
			out += '"';
			for (let i = 0; i < key.length; i++) {
				out += HEX_BYTES[key[i]];
			}
			out += '"';
		} else {
			throw new Error(
				`Unsupported key in a key list: ${typeof key === "number" ? key : typeof key}`,
			);
		}
	}
	return out + "]";
};

type SearchQueryParts = {
	query: string;
	/* orderBy: string; */
	bindable: any[];
	selects: string[];
};
type CountQueryParts = {
	query: string;
	join: string;
	bindable: any[];
	selects: string[];
};

const getOrSetRootTable = (
	joinBuilder: Map<string, JoinOrRootTable>,
	table: Table,
) => {
	const refName = createQueryTableReferenceName(table);
	let ref = joinBuilder.get(refName);
	if (ref) {
		return ref;
	}
	const join = {
		// add the root as a join even though it is not, just so we can collect the columns it will be queried
		table: table,
		type: "root" as const,
		as: table.name,
		columns: [],
	};
	joinBuilder.set(refName, join);
	return join;
};

const convertRequestToQuery = <
	T extends "iterate" | "count" | "sum" | "delete",
	R = T extends "iterate" ? SearchQueryParts : CountQueryParts,
>(
	type: T,
	request:
		| (T extends "iterate"
				? {
						query?: types.Query[];
						sort?: types.Sort[] | types.Sort;
					}
				: T extends "count"
					? {
							query?: types.Query[];
						}
					: T extends "delete"
						? {
								query?: types.Query[];
							}
						: {
								query?: types.Query[];
								key: string | string[];
							})
		| undefined,
	tables: Map<string, Table>,
	table: Table,
	extraJoin?: Map<string, JoinOrRootTable>,
	path: string[] = [],
	options?: {
		fetchAll?: boolean;
		planner?: PlanningSession;
		/** A condition AND-ed after the query; its parameters bind last. */
		extraWhere?: { sql: string; bindable: unknown[] };
		/**
		 * The root table is reached through its primary key (an `extraWhere`
		 * on it), so the planner forces no index on it.
		 */
		primaryKeyAccess?: boolean;
	},
): R => {
	let whereBuilder = "";
	let bindableBuilder: any[] = [];
	/* let orderByBuilder: string | undefined = undefined; */
	/* let tablesToSelect: string[] = [table.name]; */
	let joinBuilder: Map<string, JoinOrRootTable> = extraJoin || new Map();

	getOrSetRootTable(joinBuilder, table);

	const coercedQuery = coerceLocalQueries(request?.query);
	if (coercedQuery.length === 1) {
		const { where, bindable } = convertQueryToSQLQuery(
			coercedQuery[0],
			tables,
			table,
			joinBuilder,
			path,
			undefined,
			0,
		);
		whereBuilder += where;
		bindableBuilder.push(...bindable);
	} else if (coercedQuery.length > 1) {
		const { where, bindable } = convertQueryToSQLQuery(
			new types.And(coercedQuery),
			tables,
			table,
			joinBuilder,
			path,
			undefined,
			0,
		);
		whereBuilder += where;
		bindableBuilder.push(...bindable);
	}

	/* if (isIterateRequest(request, type)) {
		let sort = request?.sort;
		if (
			(!sort || (Array.isArray(sort) && sort.length === 0)) &&
			!options?.fetchAll
		) {
			sort =
				table.primary && path.length === 0
					? [{ key: [table.primary], direction: types.SortDirection.ASC }]
					: undefined;
		}

		if (sort) {
			let sortArr = Array.isArray(sort) ? sort : [sort];
			if (sortArr.length > 0) {
				orderByBuilder = "";
				let once = false;
				for (const sort of sortArr) {
					const { foreignTables, queryKey } = resolveTableToQuery(
						table,
						tables,
						joinBuilder,
						[...path, ...sort.key],
						undefined,
						true,
					);

					for (const foreignTable of foreignTables) {
						if (once) {
							orderByBuilder += ", ";
						}
						once = true;

						foreignTable.columns.push(queryKey); // add the sort key to the list of columns that will be used for this query

						orderByBuilder += `${foreignTable.as}.${queryKey} ${sort.direction === types.SortDirection.ASC ? "ASC" : "DESC"}`;
					}
				}
			}
		}
	} */
	if (options?.extraWhere) {
		whereBuilder =
			whereBuilder.length > 0
				? `(${whereBuilder}) AND (${options.extraWhere.sql})`
				: options.extraWhere.sql;
		bindableBuilder.push(...options.extraWhere.bindable);
	}
	const where = whereBuilder.length > 0 ? "where " + whereBuilder : undefined;

	if (extraJoin && extraJoin.size > 0) {
		insertMapIntoMap(joinBuilder, extraJoin);
	}
	let { join } = buildJoin(joinBuilder, {
		...options,
		primaryKeyTable:
			path.length === 0 &&
			(options?.primaryKeyAccess || matchesPrimaryKey(coercedQuery, table))
				? table
				: undefined,
	});

	const query = `${join ? join : ""} ${where ? where : ""}`;

	return {
		query,
		/* orderBy: orderByBuilder, */
		bindable: bindableBuilder,
	} as R;
};

/**
 * Whether a conjunct of `query` matches `table`'s primary key by equality. Such
 * a query reaches at most one row of `table` through its primary key.
 */
const matchesPrimaryKey = (query: types.Query[], table: Table): boolean => {
	if (table.primary === false) {
		return false;
	}
	for (const item of query) {
		if (item instanceof types.And) {
			if (matchesPrimaryKey(item.and, table)) {
				return true;
			}
			continue;
		}
		if (
			!(item instanceof types.StateFieldQuery) ||
			getInlineTableFieldName(item.key) !== table.primary
		) {
			continue;
		}
		if (
			item instanceof types.ByteMatchQuery ||
			(item instanceof types.StringMatch &&
				item.method === types.StringMatchMethod.exact &&
				!item.caseInsensitive) ||
			(item instanceof types.IntegerCompare &&
				item.compare === types.Compare.Equal &&
				table.primaryField?.type !== "BLOB")
		) {
			return true;
		}
	}
	return false;
};

export const buildJoin = (
	joinBuilder: Map<string, JoinOrRootTable>,
	options?: {
		planner?: PlanningSession;
		/**
		 * A root table whose primary key the query matches by equality. The
		 * primary key reaches its row, so the planner adds no index for it.
		 */
		primaryKeyTable?: Table;
	},
): { join: string } => {
	/* let joinTypeDefault = resolveAllColumns
		? "CROSS JOIN"
		: "JOIN"; */
	let join = "";

	for (const [_key, table] of joinBuilder) {
		if (table.type !== "root") {
			continue;
		}
		const out = _buildJoin(table, options);
		join += out.join;
	}
	for (const [_key, table] of joinBuilder) {
		if (table.type === "root") {
			continue;
		}
		const out = _buildJoin(table, options);
		join += out.join;
	}
	return { join };
};

const _buildJoin = (
	table: JoinOrRootTable,
	options?: {
		planner?: PlanningSession;
		primaryKeyTable?: Table;
	},
) => {
	let join = "";
	let indexedBy: string | undefined = undefined;
	if (table.type !== "root") {
		table!.columns.push(PARENT_TABLE_ID); // we unshift because we join on the parent id before where clause
	}

	if (table!.columns.length > 0) {
		const usedColumns = removeDuplicatesOrdered(table!.columns);
		const usesImplicitPrimaryKeyIndex =
			table.type === "root" &&
			table.table.primary !== false &&
			((usedColumns.length === 1 && usedColumns[0] === table.table.primary) ||
				table.table === options?.primaryKeyTable);
		if (options?.planner && !usesImplicitPrimaryKeyIndex) {
			const indexKey = options.planner.resolveIndex(
				table.table.name,
				usedColumns,
			);
			indexedBy = options.planner.forceIndex ? ` INDEXED BY ${indexKey} ` : "";
		} else {
			indexedBy = "";
		}
	}

	if (table.type !== "root") {
		let nonInlinedParent =
			table.table.parent && getNonInlinedTable(table.table.parent);
		if (!nonInlinedParent) {
			throw new Error("Unexpected: missing parent");
		}
		let joinType = table.type === "cross" ? "LEFT JOIN" : "LEFT JOIN";
		join += ` ${joinType} ${table.table.name} AS ${table.as} ${indexedBy} ON ${nonInlinedParent.name}.${nonInlinedParent.primary} = ${table.as}.${PARENT_TABLE_ID} `;
	} else if (indexedBy) {
		join += indexedBy;
	}

	return { join };
};

const insertMapIntoMap = (map: Map<string, any>, insert: Map<string, any>) => {
	for (const [key, value] of insert) {
		map.set(key, value);
	}
};

export const convertQueryToSQLQuery = (
	query: types.Query,
	tables: Map<string, Table>,
	table: Table,
	joinBuilder: Map<string, JoinOrRootTable>,
	path: string[],
	tableAlias: string | undefined,
	skipKeys: number,
): { where: string; bindable: any[] } => {
	let whereBuilder = "";
	let bindableBuilder: any[] = [];
	/* 	let tablesToSelect: string[] = []; */

	const handleAnd = (
		queries: types.Query[],
		path: string[],
		tableAlias: string | undefined,
		keysOffset: number,
	) => {
		for (const query of queries) {
			const { where, bindable } = convertQueryToSQLQuery(
				query,
				tables,
				table,
				joinBuilder,
				path,
				tableAlias,
				keysOffset,
			);
			whereBuilder =
				whereBuilder.length > 0 ? `(${whereBuilder}) AND (${where})` : where;
			bindableBuilder.push(...bindable);
		}
	};

	if (query instanceof types.StateFieldQuery) {
		const { where, bindable } = convertStateFieldQuery(
			query,
			tables,
			table,
			joinBuilder,
			path,
			tableAlias,
			skipKeys,
		);
		whereBuilder += where;
		bindableBuilder.push(...bindable);
	} else if (query instanceof types.Nested) {
		let joinPrefix = "__" + String(tables.size);
		path = [...path, ...query.path];
		let newSkipKeys = skipKeys + query.path.length;
		handleAnd(query.query, path, joinPrefix, newSkipKeys);
	} else if (query instanceof types.LogicalQuery) {
		if (query instanceof types.And) {
			handleAnd(query.and, path, tableAlias, skipKeys);
		} else if (query instanceof types.Or) {
			for (const subquery of query.or) {
				const { where, bindable } = convertQueryToSQLQuery(
					subquery,
					tables,
					table,
					joinBuilder,
					path,
					tableAlias,
					skipKeys,
				);
				whereBuilder =
					whereBuilder.length > 0 ? `(${whereBuilder}) OR(${where})` : where;
				bindableBuilder.push(...bindable);
			}
		} else if (query instanceof types.Not) {
			const { where, bindable } = convertQueryToSQLQuery(
				query.not,
				tables,
				table,
				joinBuilder,
				path,
				tableAlias,
				skipKeys,
			);
			whereBuilder = `NOT(${where})`;
			bindableBuilder.push(...bindable);
		} else {
			throw new Error("Unsupported query type: " + query.constructor.name);
		}
	} else {
		throw new Error("Unsupported query type: " + query.constructor.name);
	}

	return {
		where: whereBuilder,
		bindable: bindableBuilder,
	};
};

const cloneQuery = (query: types.StateFieldQuery) => {
	return deserialize(serialize(query), types.StateFieldQuery);
};

type JoinOrRootTable = JoinTable | RootTable;

type JoinTable = {
	table: Table;
	as: string;
	type: "left" | "cross";
	columns: string[];
};

type RootTable = {
	type: "root";
	table: Table;
	as: string;
	columns: string[];
};

/* const createQueryTableReferenceName = (
	table: Table,
	alias: string | undefined,
) => {
	
	if (
		!alias 
	) {
		let aliasSuffix =
			"_query"; //  "_" + String(joinSize); TODO this property will make every join unique, which is not wanted unless (ever?) since we can do OR in SQL which means we can do one join and perform AND/OR logic without joining multiple times to apply multiple conditions
		alias = aliasSuffix;
	}
	const tableNameAs = alias ? alias + "_" + table.name : table.name;
	return tableNameAs;
}; */

const createQueryTableReferenceName = (table: Table) => {
	return table.parent == null ? table.name : "_query_" + table.name;
};

const createReconstructReferenceName = (table: Table) => {
	return table.name; /* table.parent == null ? table.name : "_rec_" + table.name; */
};

const resolveTableToQuery = (
	table: Table,
	tables: Map<string, Table>,
	join: Map<string, JoinOrRootTable>,
	path: string[],
	alias: string | undefined,
	searchSelf: boolean,
): { queryKey: string; foreignTables: JoinOrRootTable[] } => {
	// we are matching in two ways.

	// 1. joins
	// we go down the path and resolve related tables until the last index
	// the last path value is the query key

	// 2. inline table fields
	// multiple keys in the path can correspond to a field in a inline table
	// this means we need to also check if the key is a field in the current table

	if (searchSelf) {
		const inlineName = getInlineTableFieldName(path);
		let field = table.fields.find((x) => x.name === inlineName);
		if (field) {
			return {
				queryKey: field.name,
				foreignTables: [getOrSetRootTable(join, table)],
			};
		}
	}

	let currentTables: JoinTable[] = [
		{
			table,
			as: alias || table.name,
			type: "cross" as const,
			columns: [],
		},
	];
	let prevTables: JoinTable[] | undefined = undefined;

	// outer:
	for (const [_i, key] of path /* .slice(0, -1) */
		.entries()) {
		let newTables: JoinTable[] = [];
		for (const currentTable of currentTables.map((x) => x.table)) {
			const schema = getSchema(currentTable.ctor);
			const field = schema.fields.find((x) => x.key === key)!;
			if (!field && currentTable.children.length > 0 && _i < path.length - 1) {
				// Terminal fields are selected across all variants below.
				throw new MissingFieldError(
					`Property with key "${key}" is not found in the schema ${JSON.stringify(schema.fields.map((x) => x.key))} `,
				);
			}
			for (const child of currentTable.children) {
				const tableNameAs = createQueryTableReferenceName(
					child,
					/* alias */ /* ,
					field.type,
					join.size, */
				);

				let isMatching =
					child.parentPath![child.parentPath!.length - 1] === key;
				if (isMatching) {
					const tableWithAlias = {
						columns: [],
						table: child,
						as: tableNameAs,
						type:
							currentTable.children.length > 1
								? ("left" as const)
								: ("cross" as const),
					};
					if (child.isSimpleValue) {
						if (!child.inline) {
							join.set(tableNameAs, tableWithAlias);
						}
						return {
							queryKey: FOREIGN_VALUE_PROPERTY,
							foreignTables: [tableWithAlias],
						};
					}

					newTables.push(tableWithAlias);
					if (!child.inline) {
						join.set(tableNameAs, tableWithAlias);
					}
				}
			}
		}
		prevTables = currentTables;
		currentTables = newTables;

		/* if (currentTables.length > 0 && i === path.length - 2) {
			// we are at the last key in the path
			// the next key should be the query key
			break;
		} */

		if (currentTables.length === 0) {
			currentTables = prevTables;
			break;
		}
	}

	if (currentTables.length === 0) {
		throw new Error("Unexpected");
	}

	let foreignTables: JoinTable[] = currentTables.filter((x) =>
		getSchema(x.table.ctor).fields.some(
			(field) => field.key === path[path.length - 1],
		),
	);
	if (foreignTables.length === 0) {
		throw new MissingFieldError("Failed to find field to join");
	}
	let tableToQuery: Table | undefined =
		foreignTables[foreignTables.length - 1].table;
	let queryKeyPath = [path[path.length - 1]];
	while (tableToQuery?.inline) {
		queryKeyPath.unshift(
			tableToQuery!.parentPath![tableToQuery!.parentPath!.length - 1],
		);
		tableToQuery = tableToQuery.parent;
	}

	let queryKey =
		queryKeyPath.length > 0
			? getInlineTableFieldName(queryKeyPath)
			: FOREIGN_VALUE_PROPERTY;
	return { queryKey, foreignTables };
};

const convertStateFieldQuery = (
	query: types.StateFieldQuery,
	tables: Map<string, Table>,
	table: Table,
	join: Map<string, JoinOrRootTable>,
	path: string[],
	tableAlias: string | undefined,
	skipKeys: number,
): { where: string; bindable: any[] } => {
	// if field id represented as foreign table, do join and compare
	const inlinedName = getInlineTableFieldName(query.key);
	const tableField = table.fields.find(
		(x) => x.name === inlinedName,
	); /* stringArraysEquals(query.key, [...table.parentPath, x.name]) )*/
	const isForeign = !tableField; // table.fields.find(x => x.name === query.key[query.key.length - 1])
	if (isForeign) {
		const tablePath: string[] = [...path];
		for (let i = skipKeys; i < query.key.length; i++) {
			tablePath.push(query.key[i]);
		}
		const { queryKey, foreignTables } = resolveTableToQuery(
			table,
			tables,
			join,
			tablePath,
			tableAlias,
			false,
		);
		query = cloneQuery(query);
		query.key = [queryKey];
		let whereBuilder: string[] = [];
		let bindableBuilder: any[][] = [];

		for (const ftable of foreignTables) {
			const physicalTable = getNonInlinedTable(ftable.table);
			if (physicalTable === table) {
				throw new Error("Unexpected");
			}

			const { where, bindable } = convertQueryToSQLQuery(
				query,
				tables,
				physicalTable,
				join,
				path,
				ftable.table.inline
					? createQueryTableReferenceName(physicalTable)
					: ftable.as,
				skipKeys,
			);
			whereBuilder.push(where);
			bindableBuilder.push(bindable);
		}
		return {
			where: whereBuilder.join(" OR "),
			bindable: bindableBuilder.flat(),
		};
	}

	const columnAggregator = join.get(createQueryTableReferenceName(table))!;
	if (!columnAggregator) {
		throw new Error("Unexpected");
	}
	columnAggregator.columns.push(inlinedName);

	let bindable: any[] = [];
	const keyWithTable =
		(tableAlias || table.name) + "." + escapeColumnName(inlinedName);
	let where: string;
	if (query instanceof types.StringMatch) {
		let statement = "";

		if (query.method === types.StringMatchMethod.contains) {
			statement = `${keyWithTable} LIKE ? `;
			bindable.push(`%${query.value}%`);
		} else if (query.method === types.StringMatchMethod.prefix) {
			statement = `${keyWithTable} LIKE ? `;
			bindable.push(`${query.value}%`);
		} else if (query.method === types.StringMatchMethod.exact) {
			statement = `${keyWithTable} = ?`;
			bindable.push(`${query.value}`);
		}
		if (query.caseInsensitive) {
			statement += " COLLATE NOCASE";
		}
		where = statement;
	} else if (query instanceof types.ByteMatchQuery) {
		// compare Blob compule with f.value

		const statement = `${keyWithTable} = ?`;
		bindable.push(query.value);
		where = statement;
	} else if (query instanceof types.IntegerCompare) {
		if (tableField!.type === "BLOB") {
			// TODO perf
			where = `hex(${keyWithTable}) LIKE ? `;
			bindable.push(
				`%${toHexString(new Uint8Array([Number(query.value.value)]))}%`,
			);
		} else {
			if (query.compare === types.Compare.Equal) {
				where = `${keyWithTable} = ?`;
			} else if (query.compare === types.Compare.Greater) {
				where = `${keyWithTable} > ? `;
			} else if (query.compare === types.Compare.Less) {
				where = `${keyWithTable} <?`;
			} else if (query.compare === types.Compare.GreaterOrEqual) {
				where = `${keyWithTable} >= ? `;
			} else if (query.compare === types.Compare.LessOrEqual) {
				where = `${keyWithTable} <= ? `;
			} else {
				throw new Error(`Unsupported compare type: ${query.compare} `);
			}

			if (unwrapNestedType(tableField.from!.type) === "u64") {
				// shift left because that is how we insert the value
				bindable.push(u64ToI64(query.value.value));
			} else {
				bindable.push(query.value.value);
			}
		}
	} else if (query instanceof types.IsNull) {
		where = `${keyWithTable} IS NULL`;
	} else if (query instanceof types.BoolQuery) {
		where = `${keyWithTable} = ?`;
		bindable.push(query.value ? 1 : 0);
	} else {
		throw new Error("Unsupported query type: " + query.constructor.name);
	}
	return { where, bindable };
};

const removeDuplicatesOrdered = (arr: string[]) => {
	let seen = new Set();
	return arr.filter((item) => {
		if (seen.has(item)) {
			return false;
		}
		seen.add(item);
		return true;
	});
};
