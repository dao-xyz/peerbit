all: build

deps:
	npm install

test: deps
	npm run test
	
build: test
	mkdir -p examples/browser/lib/
	npm run build
	@echo "Build success!"
	@echo "Output: 'dist/', 'examples/browser/'"

clean:
	rm -rf orbitdb/
	rm -rf node_modules/

clean-dependencies: clean
	rm -f package-lock.json;

rebuild: | clean-dependencies build

.PHONY: test build
