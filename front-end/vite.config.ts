import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../back-end/static",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    // Use polling if changes don't trigger HMR (e.g. some network drives / editors)
    watch: { usePolling: true },
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
