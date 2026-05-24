import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        surface: "#0b1220",
        panel: "#121b2d",
        border: "#22314d",
        accent: "#22c55e",
        warning: "#f59e0b",
        danger: "#ef4444",
        muted: "#93a4bf",
      },
      fontFamily: {
        sans: ["'Avenir Next'", "'Segoe UI'", "sans-serif"],
        mono: ["'IBM Plex Mono'", "'SFMono-Regular'", "monospace"],
      },
      boxShadow: {
        panel: "0 12px 30px rgba(2, 6, 23, 0.35)",
      },
    },
  },
  plugins: [],
};

export default config;
