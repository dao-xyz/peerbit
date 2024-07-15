import { getSchema } from "@dao-xyz/borsh";

export const copySerialization = (sourceClazz: any, targetClazz: any) => {

    const copiedFromAlready: any[] = targetClazz["__copiedFrom"] || [];
    if (copiedFromAlready?.includes(sourceClazz)) {
        return;
    }

    copiedFromAlready.push(sourceClazz)
    targetClazz["__copiedFrom"] = copiedFromAlready;

    const targetSchema = getSchema(targetClazz)
    const sourceSchema = getSchema(sourceClazz)

    targetSchema.fields = [...sourceSchema.fields, ...targetSchema.fields];
    targetSchema.variant = sourceSchema.variant;
    targetSchema.getDependencies = sourceSchema.getDependencies.bind(sourceSchema)
}