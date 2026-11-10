import { defineConfig, loadEnv } from 'vite';

export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, process.cwd(), '');
    const backend = env.BACKEND_API_URL || 'http://127.0.0.1:8080';
    const proxy = {
        '/api/backend/health': {
            target: backend, changeOrigin: true,
            rewrite: () => '/actuator/health',
        },
        '/api/processor/health': {
            target: env.PROCESSOR_API_URL || 'http://127.0.0.1:8083', changeOrigin: true,
            rewrite: () => '/actuator/health',
        },
        '/api/processor/pipeline': {
            target: env.PROCESSOR_API_URL || 'http://127.0.0.1:8083', changeOrigin: true,
            rewrite: () => '/api/pipeline',
        },
        '/api/simulation': { target: backend, changeOrigin: true },
        '/api/runs': {
            target: env.GENERATOR_API_URL || 'http://127.0.0.1:8081',
            changeOrigin: true,
            // SSE is a long-lived HTTP stream; it does not need a WebSocket proxy.
            timeout: 0,
            proxyTimeout: 0,
        },
    };

    return {
        server: { host: '127.0.0.1', port: 5173, strictPort: true, proxy },
        preview: { host: '127.0.0.1', port: 4173, strictPort: true, proxy },
    };
});
