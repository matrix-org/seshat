import {
    initThreadPool,
} from '../bindings/seshat-wasm/pkg/seshat.js';

const worker = new Worker(new URL('./worker/index.ts', import.meta.url), { type: 'module' });

worker.onmessage = async (event) => {
    console.log(event);
    console.log({ client_data: event.data });
};

worker.postMessage('search');
window.seshatWorker = worker