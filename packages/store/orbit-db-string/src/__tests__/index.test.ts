import { Range } from "../range.js";
import { PayloadOperation, applyOperations } from "../string-index.js";

describe('operations', () => {
    it('add', () => {
        const operations: PayloadOperation[] = [
            {
                index: new Range({
                    offset: 0
                }),
                value: 'hello'
            },
            {
                index: new Range({
                    offset: 'hello'.length
                }),
                value: ' '
            },
            {
                index: new Range({
                    offset: 'hello '.length,
                }),
                value: 'world'
            }
        ]

        let string = applyOperations('', operations.map((v, ix) => {
            {
                return {
                    hash: ix.toString(),
                    payload: v
                } as any
            }
        }))
        expect(string).toEqual('hello world');
    })

    it('replace', () => {
        const operations: PayloadOperation[] = [
            {
                index: new Range({
                    offset: 0
                }),
                value: 'hello'
            },
            {
                index: new Range({
                    offset: 'hello'.length
                }),
                value: 'w'
            },
            {
                index: new Range({
                    offset: 'hello '.length
                }),
                value: 'world'
            },
            {
                index: new Range({
                    offset: 'hello'.length
                }),
                value: ' '
            }
        ]

        let string = applyOperations('', operations.map((v, ix) => {
            {
                return {
                    hash: ix.toString(),
                    payload: v
                } as any
            }
        }))
        expect(string).toEqual('hello world');
    })

    it('delete', () => {
        const operations: PayloadOperation[] = [
            {
                index: new Range({
                    offset: 0,
                    length: 0
                }),
                value: 'hello world'
            },
            {
                index: new Range({
                    offset: 'hello'.length,
                    length: 'hello world'.length
                }),
            }
        ]

        let string = applyOperations('', operations.map((v, ix) => {
            {
                return {
                    hash: ix.toString(),
                    payload: v
                } as any
            }
        }))
        expect(string).toEqual('hello');
    })
})