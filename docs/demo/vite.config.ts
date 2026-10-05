import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import wasm from 'vite-plugin-wasm'

export default defineConfig({
  base: './',
  build: { outDir: '../demo-assets', emptyOutDir: true },
  plugins: [react(), wasm()],
  server: {
    port: 3001,
  },
})
