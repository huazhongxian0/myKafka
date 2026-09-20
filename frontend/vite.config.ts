import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
      '/ws': {
        target: 'ws://localhost:8081',
        ws: true,
      },
      // Orders consumer ports
      '/consumer-port-8082': {
        target: 'http://localhost:8082',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8082/, ''),
      },
      '/consumer-port-8083': {
        target: 'http://localhost:8083',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8083/, ''),
      },
      '/consumer-port-8084': {
        target: 'http://localhost:8084',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8084/, ''),
      },
      // Addresses consumer ports
      '/consumer-port-8085': {
        target: 'http://localhost:8085',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8085/, ''),
      },
      '/consumer-port-8086': {
        target: 'http://localhost:8086',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8086/, ''),
      },
      '/consumer-port-8087': {
        target: 'http://localhost:8087',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8087/, ''),
      },
      // Payments consumer ports
      '/consumer-port-8088': {
        target: 'http://localhost:8088',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8088/, ''),
      },
      '/consumer-port-8089': {
        target: 'http://localhost:8089',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8089/, ''),
      },
      '/consumer-port-8090': {
        target: 'http://localhost:8090',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/consumer-port-8090/, ''),
      },
    },
  },
})
