import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Backend serves TLS with a self-signed certificate from data/certs/ unless
// a real certificate is placed there, so the dev proxy must not verify it.
const backendTarget = 'https://127.0.0.1:8000'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/api': { target: backendTarget, secure: false },
      '/health': { target: backendTarget, secure: false },
      '/favicon.ico': { target: backendTarget, secure: false },
      '/brand-assets': { target: backendTarget, secure: false },
    },
  },
  build: {
    outDir: 'dist',
    assetsDir: 'app-assets',
    emptyOutDir: true,
    sourcemap: true,
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
    css: true,
  },
})
