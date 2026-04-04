/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  define: {
    __APP_VERSION__: JSON.stringify(process.env.VITE_COMMIT_SHA || 'dev'),
  },
  test: {
    exclude: ['functions/**', 'pipelines/**', 'node_modules/**'],
  },
})
