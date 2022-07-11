import { PayloadOperation, applyOperations, StringLogEntry } from "../string-index";

describe('operations', () => {
    test('add', () => {
        const operations: PayloadOperation[] = [
            {
                index: {
                    offset: 0
                },
                value: 'hello'
            },
            {
                index: {
                    offset: 'hello'.length
                },
                value: ' '
            },
            {
                index: {
                    offset: 'hello '.length,
                },
                value: 'world'
            }
        ]

        let string = applyOperations('', operations.map((v, ix) => {
            {
                return {
                    hash: ix.toString(),
                    payload: v
                } as StringLogEntry
            }
        }))
        expect(string).toEqual('hello world');
    })

    test('replace', () => {
        const operations: PayloadOperation[] = [
            {
                index: {
                    offset: 0
                },
                value: 'hello'
            },
            {
                index: {
                    offset: 'hello'.length
                },
                value: 'w'
            },
            {
                index: {
                    offset: 'hello '.length
                },
                value: 'world'
            },
            {
                index: {
                    offset: 'hello'.length
                },
                value: ' '
            }
        ]

        let string = applyOperations('', operations.map((v, ix) => {
            {
                return {
                    hash: ix.toString(),
                    payload: v
                } as StringLogEntry
            }
        }))
        expect(string).toEqual('hello world');
    })

    test('delete', () => {
        const operations: PayloadOperation[] = [
            {
                index: {
                    offset: 0,
                    length: 0
                },
                value: 'hello world'
            },
            {
                index: {
                    offset: 'hello'.length,
                    length: 'hello world'.length
                },
            }
        ]

        let string = applyOperations('', operations.map((v, ix) => {
            {
                return {
                    hash: ix.toString(),
                    payload: v
                } as StringLogEntry
            }
        }))
        expect(string).toEqual('hello');
    })
})