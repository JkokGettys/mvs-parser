import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/admin/',
  server: {
    port: 3001,
    proxy: {
      '/pdf-versions': 'http://localhost:8000',
      '/parse-version': 'http://localhost:8000',
      '/parsers': 'http://localhost:8000',
      '/stats': 'http://localhost:8000',
      '/health': 'http://localhost:8000',
      '/parse': 'http://localhost:8000',
      '/import': 'http://localhost:8000',
    },
  },
})
