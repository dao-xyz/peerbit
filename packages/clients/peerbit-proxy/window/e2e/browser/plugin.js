import fs from 'fs';
import path from 'path';

export default function copyToPublicPlugin(options) {
    return {
        name: 'copy-to-public',
        buildStart() {
            if (options && options.assets) {
                options.assets.forEach(({ src, dest }) => {
                    const sourcePath = path.resolve(src);
                    const destinationPath = path.resolve(process.cwd(), 'public', dest);

                    copyAssets(sourcePath, destinationPath);
                });
            }
        }
    };
}

function copyAssets(srcPath, destPath) {
    if (fs.statSync(srcPath).isDirectory()) {
        // Ensure the directory exists in the public folder
        fs.mkdirSync(destPath, { recursive: true });

        // Copy each file/directory inside the current directory
        fs.readdirSync(srcPath).forEach(file => {
            const srcFilePath = path.join(srcPath, file);
            const destFilePath = path.join(destPath, file);
            copyAssets(srcFilePath, destFilePath);  // Recursion for directories
        });
    } else {
        // Ensure the destination directory exists
        fs.mkdirSync(path.dirname(destPath), { recursive: true });

        // Copy the file
        fs.copyFileSync(srcPath, destPath);
    }
}