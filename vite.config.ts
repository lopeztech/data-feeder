import { defineConfig } from 'vite'
import { execSync } from 'child_process'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const commitHash = process.env.VITE_COMMIT_SHA
  ?? (() => { try { return execSync('git rev-parse --short HEAD').toString().trim() } catch { return 'dev' } })()

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  define: {
    __APP_VERSION__: JSON.stringify(commitHash),
  },
  test: {
    exclude: ['functions/**', 'pipelines/**', 'node_modules/**'],
  },
})
