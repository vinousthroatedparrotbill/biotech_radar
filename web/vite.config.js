import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 개발 중 /api 호출은 FastAPI(8000)로 프록시
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': 'http://localhost:8000' },
  },
})
