# IPFS Log benchmark runner

## Usage: 

From the project root, run:

```
node --expose-gc benchmarks/runner/index.js [options]
```

## Options

- `--help, -h` [boolean] Show this help
- `--baseline, -b` [boolean] Run baseline benchmarks only
- `--report, -r` [boolean] Output report (Default: false)
- `--list, -l` [boolean] List all benchmarks
- `--grep, -g` <regexp> Regular expression used to match benchmarks (Default: /.*/)
- `--stressLimit` <Int or Infinity> seconds to run a stress benchmark (Default: 300)
- `--baselineLimit` <Int> benchmark cycle limit for baseline benchmarks (Default: 1000)
- `--logLimit` <Int> max log size used for baseline benchmarks (inclusive) (Default: 10000)

## Examples:
  
```JavaScript
index.js -r -g append-baseline     Run a single benchmark (append-baseline)
index.js -r -g values-.*-baseline  Run all of the values baseline benchmarks
```
