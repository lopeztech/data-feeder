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
        ? (() => {
            const now = new Date();
            const tz = { timeZone: 'Australia/Sydney' } as const;
            const date = now.toLocaleDateString('en-GB', { ...tz, day: '2-digit', month: '2-digit', year: 'numeric' });
            const time = now.toLocaleTimeString('en-GB', { ...tz, hour: '2-digit', minute: '2-digit', hour12: false });
            return `${time} ${date}`;
          })()
        : 'dev'
    ),
  },
  test: {
    exclude: ['functions/**', 'pipelines/**', 'node_modules/**'],
  },
})
