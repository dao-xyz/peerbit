import { AbstractType, Constructor, getSchemasBottomUp } from "@dao-xyz/borsh";

export const getValuesWithType = <T>(from: any, type: Constructor<T> | AbstractType<T>, stopAtType?: Constructor<any> | AbstractType<any>): T[] => {
    const schemas = getSchemasBottomUp(from.constructor);
    const values: T[] = [];
    for (const schema of schemas) {
        for (const field of schema.schema.fields) {
            const value = from[field.key];
            if (!value) {
                continue;
            }
            const p = (element) => {
                if (element && element instanceof type) {
                    values.push(element);
                }
                else if (typeof element === 'object') {
                    if (stopAtType && element instanceof stopAtType) {
                        return;
                    }
                    const nestedValues = getValuesWithType(element, type, stopAtType);
                    nestedValues.forEach((v) => {
                        values.push(v);
                    })
                }
            }
            if (Array.isArray(value)) {
                for (const element of value) {
                    p(element)
                }
            }
            else {
                p(value);
            }
        }
    }
    return values;
}
