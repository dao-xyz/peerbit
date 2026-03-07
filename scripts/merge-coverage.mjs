import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');
const nycOutputDir = path.join(rootDir, '.nyc_output');
const coverageFilePriority = ['coverage-remapped.json', 'coverage-final.json', 'coverage-pw.json'];

if (fs.existsSync(nycOutputDir)) {
  fs.rmSync(nycOutputDir, { recursive: true, force: true });
}
fs.mkdirSync(nycOutputDir);

let counter = 0;

function findAndCopyCoverage(dir) {
  const files = fs.readdirSync(dir);
  for (const file of files) {
    const fullPath = path.join(dir, file);
    const stat = fs.statSync(fullPath);
    if (stat.isDirectory()) {
      if (file === '.coverage') {
        const selectedFile = coverageFilePriority
          .map((name) => path.join(fullPath, name))
          .find((candidate) => fs.existsSync(candidate));

        if (selectedFile) {
          const dest = path.join(nycOutputDir, `coverage-${counter++}.json`);
          fs.copyFileSync(selectedFile, dest);
          console.log(`Copied ${selectedFile.replace(rootDir + '/', '')} to .nyc_output/coverage-${counter - 1}.json`);
        }
      } else if (file !== 'node_modules' && file !== '.git' && file !== '.nyc_output') {
        findAndCopyCoverage(fullPath);
      }
    }
  }
}

findAndCopyCoverage(rootDir);
console.log(`Found and merged ${counter} coverage files.`);
