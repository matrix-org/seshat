import { defineConfig, searchForWorkspaceRoot } from 'vite'

let root = searchForWorkspaceRoot(process.cwd())
export default defineConfig({
    plugins: [
        {
            name: "isolation",
            configureServer(server) {
                server.middlewares.use((_req, res, next) => {
                    res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
                    res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
                    next();
                });
            },
        },
    ],
    server: {
        fs: {
            allow: [
                // search up for workspace root
                root,
                // your custom rules
                `${root}/../bindings/seshat-wasm/pkg/`,
            ],
        },
    },
})