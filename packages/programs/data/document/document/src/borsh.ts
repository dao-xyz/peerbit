import {
	type CustomField,
	type SimpleField,
	field,
	getSchema,
} from "@dao-xyz/borsh";

const serializedFieldAccessors = new WeakMap<
	object,
	ReadonlyMap<string, PropertyDescriptor>
>();
const copiedSerializations = new WeakMap<Function, ReadonlySet<Function>>();

/**
 * Register a Borsh field backed by an accessor and explicitly preserve that
 * accessor when Documents wraps an indexed value with its local context.
 *
 * The accessor must not inherit from another Borsh-decorated class, must be
 * defined directly on `clazz.prototype`, have both a getter and setter, and be
 * safe when invoked with the generated wrapper as `this`. In particular, it
 * cannot depend on private fields, constructor-only state, or state stored in a
 * WeakMap keyed by instances of `clazz`. The setter is invoked while Borsh
 * deserializes persisted index rows.
 */
export const registerIndexFieldAccessor = (
	clazz: Function,
	key: string,
	properties: SimpleField | CustomField<any>,
): void => {
	if (typeof key !== "string") {
		throw new Error("Index field accessor keys must be strings");
	}

	const prototype = clazz.prototype;
	const descriptor = Object.getOwnPropertyDescriptor(prototype, key);
	if (!descriptor?.get || !descriptor.set) {
		throw new Error(
			`Index field accessor '${key}' must define both a getter and setter`,
		);
	}

	const registered = serializedFieldAccessors.get(prototype);
	if (registered?.has(key)) {
		return;
	}
	let parent = Object.getPrototypeOf(clazz);
	while (parent && parent !== Function.prototype) {
		if (getSchema(parent)) {
			throw new Error(
				"Index field accessors cannot inherit from a Borsh-decorated class",
			);
		}
		parent = Object.getPrototypeOf(parent);
	}
	if (getSchema(clazz)?.fields.some((existing) => existing.key === key)) {
		throw new Error(
			`Index field accessor '${key}' is already registered as a Borsh field`,
		);
	}

	(field(properties) as unknown as PropertyDecorator)(prototype, key);
	serializedFieldAccessors.set(
		prototype,
		new Map([...(registered ?? []), [key, descriptor]]),
	);
};

const getSerializedFieldAccessor = (
	sourceClazz: any,
	targetClazz: any,
	targetSchema: ReturnType<typeof getSchema>,
	key: string,
): PropertyDescriptor | undefined => {
	const sourcePrototype = sourceClazz.prototype;
	const registeredDescriptor = serializedFieldAccessors
		.get(sourcePrototype)
		?.get(key);
	if (!registeredDescriptor) {
		return undefined;
	}

	if (targetSchema.fields.some((field) => field.key === key)) {
		throw new Error(
			`Cannot preserve index field accessor '${key}': the wrapper schema already contains that field`,
		);
	}
	if (Object.getOwnPropertyDescriptor(targetClazz.prototype, key)) {
		throw new Error(
			`Cannot preserve index field accessor '${key}': the wrapper prototype already defines it`,
		);
	}

	const descriptor = Object.getOwnPropertyDescriptor(sourcePrototype, key);
	if (!descriptor?.get || !descriptor.set) {
		throw new Error(
			`Registered index field accessor '${key}' must define both a getter and setter`,
		);
	}
	if (
		descriptor.get !== registeredDescriptor.get ||
		descriptor.set !== registeredDescriptor.set ||
		descriptor.enumerable !== registeredDescriptor.enumerable ||
		descriptor.configurable !== registeredDescriptor.configurable
	) {
		throw new Error(
			`Registered index field accessor '${key}' changed after registration`,
		);
	}
	return descriptor;
};

export const copySerialization = (sourceClazz: any, targetClazz: any) => {
	const copiedFromAlready = copiedSerializations.get(targetClazz);
	if (copiedFromAlready?.has(sourceClazz)) {
		return;
	}

	const targetSchema = getSchema(targetClazz);
	const sourceSchema = getSchema(sourceClazz);
	const accessors: Array<[string, PropertyDescriptor]> = [];
	for (const sourceField of sourceSchema.fields) {
		const descriptor = getSerializedFieldAccessor(
			sourceClazz,
			targetClazz,
			targetSchema,
			sourceField.key,
		);
		if (descriptor) {
			accessors.push([sourceField.key, descriptor]);
		}
	}
	if (accessors.length > 0 && !Object.isExtensible(targetClazz.prototype)) {
		throw new Error(
			"Cannot preserve index field accessors: the wrapper prototype is not extensible",
		);
	}

	for (const [key, descriptor] of accessors) {
		Object.defineProperty(targetClazz.prototype, key, descriptor);
	}

	targetSchema.fields = [...sourceSchema.fields, ...targetSchema.fields];
	targetSchema.variant = sourceSchema.variant;
	targetSchema.getDependencies =
		sourceSchema.getDependencies.bind(sourceSchema);
	copiedSerializations.set(
		targetClazz,
		new Set([...(copiedFromAlready ?? []), sourceClazz]),
	);
};
