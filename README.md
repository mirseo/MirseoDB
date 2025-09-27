# MirseoDB
MirseoDB - New Type Database

## Development

- Install Rust toolchain (edition 2021) and Node.js 18+ with npm.
- Install console dependencies once with `npm install` inside `console/`.
- Run the backend and console together with `cargo run`; the Rust server proxies the SvelteKit dev console on `http://127.0.0.1:3306/` (the underlying dev server still runs on `http://localhost:5173`).
- Set `MIRSEODB_SKIP_CONSOLE=1` when running `cargo run` if you want to disable the web console startup.

API requests should now target `POST /api/query` (the old `POST /query` path remains available for compatibility).

The console scaffold lives in `console/` and is built with Svelte 5 / SvelteKit. Modify `console/src/routes/+page.svelte` to start building the Supabase-style experience.
