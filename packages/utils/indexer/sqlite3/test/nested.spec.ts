/* describe("xxx", () => {
    describe("nested", () => {
        beforeEach(async () => {
            await setup({ schema: DocumentWithNesting });
        });

        it("nested", async () => {
            await store.put(new DocumentWithNesting({
                id: '1',
                nested: new Nested({ number: 1n })
            }))
            await store.put(new DocumentWithNesting({
                id: '2',
                nested: new Nested({ number: 2n })
            }))

            const response = await search(store,
                new SearchRequest({
                    query: [
                        new IntegerCompare({
                            key: ["nested", "number"],
                            compare: Compare.GreaterOrEqual,
                            value: 2n
                        })
                    ]
                })
            );
            expect(response.results).to.have.length(1);
            expect(response.results[0].value.id).to.equal("2");
        });
    })
}) */