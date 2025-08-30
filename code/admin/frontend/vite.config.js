import { defineConfig } from "vite";
import { resolve } from "node:path";

export default defineConfig({
  root: ".",
  build: {
    outDir: resolve(__dirname, "../static"),
    emptyOutDir: false, 
    assetsDir: "assets",
    rollupOptions: {
      input: {
        main: resolve(__dirname, "src/main.js"),
      },
      output: {
        entryFileNames: "assets/main.js",
        chunkFileNames: "assets/[name].js",
        assetFileNames: ({ name }) => {
          if (name && name.endsWith(".css")) return "assets/main.css";
          return "assets/[name][extname]";
        }
      }
    }
  },
  server: { port: 5173 }
});