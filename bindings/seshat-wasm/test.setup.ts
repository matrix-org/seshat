
import fs from 'node:fs/promises';
import path from 'path';

globalThis.fetch = (url): Promise<Buffer> => {
    const wasmPath = path.resolve(__dirname, './pkg/seshat_bg.wasm');
    return fs.readFile(wasmPath);
};