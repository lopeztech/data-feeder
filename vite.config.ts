/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  define: {
    __APP_VERSION__: JSON.stringify(
      process.env.VITE_COMMIT_SHA
        ? new Date().toLocaleString('en-AU', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'Australia/Sydney' })
        : 'dev'
    ),
  },
  test: {
    exclude: ['functions/**', 'pipelines/**', 'node_modules/**'],
  },
})
