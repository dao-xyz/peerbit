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
	if (!clazzes) {
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

	const addJoinFields =
		primary === false
			? addJoinFieldFromParent
			: (fields: SQLField[], contstraints: SQLConstraint[]) => {
					// we resolve primary field here since it might be unknown until this point
					const parentPrimaryField =
						primary != null
							? sqlFields.find((field) => field.name === primary)
							: undefined;
					const parentPrimaryFieldName =
						parentPrimaryField?.name || CHILD_TABLE_ID;
					const parentPrimaryFieldType = parentPrimaryField
						? parentPrimaryField.type
						: "INTEGER";

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
							definition: `${PARENT_TABLE_ID} ${parentPrimaryFieldType}`,
							type: parentPrimaryFieldType,
							from: parentPrimaryField?.from,
							unwrappedType: parentPrimaryField?.unwrappedType,
							isPrimary: false,
							path: [PARENT_TABLE_ID],
						},
					);
					contstraints.push({
						name: `${PARENT_TABLE_ID}_fk`,
						definition: `CONSTRAINT ${PARENT_TABLE_ID}_fk FOREIGN KEY(${PARENT_TABLE_ID}) REFERENCES ${tableName}(${parentPrimaryFieldName}) ON DELETE CASCADE`,
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
		key: string,
		field: Field,
		type: FieldType,
		isOptional: boolean,
	) => {
		let keyString = getInlineTableFieldName(path.slice(1), key);

		const isPrimary = primary != null && keyString === primary;
		foundPrimary = foundPrimary || isPrimary;

		const fieldType = toSQLType(type, isOptional);
		sqlFields.push({
			name: keyString,
			key,
			definition: `${escapeColumnName(keyString)} ${fieldType} ${isPrimary ? "PRIMARY KEY" : ""}`,
			type: fieldType,
			isPrimary,
			from: field,
			unwrappedType: unwrapNestedType(field.type),
			path: [...path.slice(1), key],
		});
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
			handleSimpleField(key, field, type, true);
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
		const testCtors = value?.constructor
			? [value?.constructor]
			: ([
					unwrapNestedType(field.type),
					...(getDependencies(unwrapNestedType(field.type) as any, 0) || []),
				] as Constructor<any>[]);
		for (const ctor of testCtors) {
			if (!ctor) {
				continue;
			}
			const schema = getSchema(ctor);
			if (!schema) {
				continue;
			}
			if (ctor) {
				clazzName = ctor;
				break;
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
	insertFn: (values: any[], table: Table) => Promise<any> | any,
	obj: Record<string, any>,
	tables: Map<string, Table>,
	table: Table,
	fields: Field[],
	handleNestedCallback?: (
		cb: (parentId: any) => Promise<void>,
	) => Promise<void> | void | number,
	parentId: any = undefined,
	index?: number,
): Promise<void> => {
	const bindableValues: any[] = [];
	let nestedCallbacks: ((id: any) => Promise<void>)[] = [];

	handleNestedCallback =
		table.primary === false
			? handleNestedCallback
			: (fn) => nestedCallbacks.push(fn);

	const handleElement = async (
		item: any,
		field: Field,
		parentId: any,
		index?: number,
	) => {
		const subTable = getTableFromValue(table, tables, field, item);

		await insert(
			insertFn,
			typeof item === "function" && item instanceof Uint8Array === false
				? item
				: subTable.isSimpleValue
					? // eslint-disable-next-line new-cap
						new subTable.ctor(item)
					: Object.assign(Object.create(subTable.ctor.prototype), item),
			tables,
			subTable,
			getSchema(subTable.ctor).fields,
			handleNestedCallback,
			parentId,
			index,
		);
	};

	const handleNested = async (
		field: Field,
		optional: boolean,
		parentId: any,
	) => {
		if (Array.isArray(obj[field.key])) {
			const arr = obj[field.key];
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

	const thisId = await insertFn(bindableValues, table);
	if (table.primary === false && nestedCallbacks.length > 0) {
		throw new Error("Unexpected");
	}
	await Promise.all(nestedCallbacks.map((x) => x(thisId)));

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
): { sql: string; bindable: any[] } => {
	const { query, bindable } = convertRequestToQuery(
		"delete",
		{ query: types.toQuery(request.query) },
		tables,
		table,
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
): { sql: string; bindable: any[] } => {
	const { query, bindable } = convertRequestToQuery(
		"sum",
		{ query: types.toQuery(request.query), key: request.key },
		tables,
		table,
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
		{ query: request?.query ? types.toQuery(request.query) : undefined },
		tables,
		table,
	);
	return {
		sql: `SELECT count(DISTINCT ${table.name}.${table.primary!}) as count FROM ${table.name} ${query}`,
		bindable,
	};
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

					resolverBuilder.push({
						from: `${table.name}.${escapeColumnName(queryKey)}`,
						as: `'${foreignTable.as}#${queryKey}'`,
					});
				}
			}
		}
	}

	return { orderByBuilder };
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
	let unionBuilder = "";
	let orderByClause: string = "";

	let matchedOnce = false;
	let lastError: Error | undefined = undefined;

	const selectsPerTable = selectAllFieldsFromTables(rootTables, options?.shape);
	let bindableBuilder: any[] = [];

	for (const [i, table] of rootTables.entries()) {
		const { selects, joins, groupBy } = selectsPerTable[i];

		try {
			const { orderByBuilder } = buildOrderBy(
				request?.sort,
				tables,
				table,
				joins,
				selects,
				[],
				options,
			);

			if (!orderByClause && orderByBuilder) {
				// assume all order by clauses will be the same
				orderByClause =
					orderByBuilder.length > 0
						? orderByClause.length > 0
							? orderByClause + ", " + orderByBuilder
							: orderByBuilder
						: orderByClause;
			}

			//orderByAddedOnce = true;
		} catch (error) {
			if (error instanceof MissingFieldError) {
				lastError = error;
				continue;
			}
			throw error;
		}

		const selectQuery = generateSelectQuery(table, selects);

		for (const flattenRequest of flattenQuery(request)) {
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
				matchedOnce = true;
				bindableBuilder.push(...bindable);
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					orderByClause = "";
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
	},
): R => {
	let whereBuilder = "";
	let bindableBuilder: any[] = [];
	/* let orderByBuilder: string | undefined = undefined; */
	/* let tablesToSelect: string[] = [table.name]; */
	let joinBuilder: Map<string, JoinOrRootTable> = extraJoin || new Map();

	getOrSetRootTable(joinBuilder, table);

	const coercedQuery = types.toQuery(request?.query);
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
	const where = whereBuilder.length > 0 ? "where " + whereBuilder : undefined;

	if (extraJoin && extraJoin.size > 0) {
		insertMapIntoMap(joinBuilder, extraJoin);
	}
	let { join } = buildJoin(joinBuilder, options);

	const query = `${join ? join : ""} ${where ? where : ""}`;

	return {
		query,
		/* orderBy: orderByBuilder, */
		bindable: bindableBuilder,
	} as R;
};

export const buildJoin = (
	joinBuilder: Map<string, JoinOrRootTable>,
	options?: {
		planner?: PlanningSession;
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
	},
) => {
	let join = "";
	let indexedBy: string | undefined = undefined;
	if (table.type !== "root") {
		table!.columns.push(PARENT_TABLE_ID); // we unshift because we join on the parent id before where clause
	}

	if (table!.columns.length > 0) {
		const usedColumns = removeDuplicatesOrdered(table!.columns);
		indexedBy = options?.planner
			? ` INDEXED BY ${options.planner.resolveIndex(table.table.name, usedColumns)} `
			: "";
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
			if (!field && currentTable.children.length > 0) {
				// second arg is needed because of polymorphic fields we might end up here intentially to check what tables to query
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
		x.table.fields.find((x) => x.key === path[path.length - 1]),
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
			if (ftable.table === table) {
				throw new Error("Unexpected");
			}

			const { where, bindable } = convertQueryToSQLQuery(
				query,
				tables,
				ftable.table,
				join,
				path,
				ftable.as,
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
