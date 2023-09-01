import {
	AbstractType,
	Constructor,
	getSchema,
	StructKind
} from "@dao-xyz/borsh";

const MAX_PROTOTYPE_SEARCH = 500;
const PROTOTYPE_DESERIALIZATION_HANDLER_OFFSET = 500;
const PROTOTYPE_DEPENDENCY_HANDLER_OFFSET =
	PROTOTYPE_DESERIALIZATION_HANDLER_OFFSET + MAX_PROTOTYPE_SEARCH;
const getDependencies = <T>(
	ctor: Constructor<T> | AbstractType<T>,
	offset: number
): Constructor<T> | AbstractType<T> | undefined =>
	ctor.prototype[PROTOTYPE_DEPENDENCY_HANDLER_OFFSET + offset];

const getSchemasBottomUp = <T>(
	ctor: Constructor<T> | AbstractType<T>
): StructKind[] => {
	let last: StructKind | undefined = undefined;
	const ret: StructKind[] = [];
	for (let i = 0; i < 1000; i++) {
		const curr = getSchema(ctor, i);
		if (!curr) {
			if (last && !getDependencies(ctor, i)?.length) {
				return ret;
			}
		} else {
			ret.push(curr);
			last = curr;
		}
	}
	return ret;
};

export const getValuesWithType = <T>(
	from: any,
	type: Constructor<T> | AbstractType<T>,
	stopAtType?: Constructor<any> | AbstractType<any>
): T[] => {
	const schemas = getSchemasBottomUp(from.constructor);
	const values: T[] = [];
	for (const schema of schemas) {
		for (const field of schema.fields) {
			const value = from[field.key];
			if (!value) {
				continue;
			}
			const p = (element) => {
				if (element && element instanceof type) {
					values.push(element as T);
				} else if (typeof element === "object") {
					if (stopAtType && element instanceof stopAtType) {
						return;
					}
					const nestedValues = getValuesWithType(element, type, stopAtType);
					nestedValues.forEach((v) => {
						values.push(v);
					});
				}
			};
			if (Array.isArray(value)) {
				for (const element of value) {
					p(element);
				}
			} else {
				p(value);
			}
		}
	}
	return values;
};
