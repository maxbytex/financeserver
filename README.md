# Finance server

A finance server that helps you manage your portfolio and expenses using an
OpenAPI-documented API and MCP tools.

[![Deploy on Deno](https://deno.com/button)](https://console.deno.com/new?clone=https://github.com/MiguelRipoll23/financeserver&predeploy=deno%20task%20push)

## Configuration

Follow the steps below after using the Deploy button above this section:

1. On the Deno Deploy project page, go to Settings → Environment Variables.
2. Copy `.env.example` to `.env` (Deno Deploy requires the `.env` extension when importing).
3. Drag and drop the `.env` file onto the Environment Variables panel, or click
  Import and select the file.

### Database configuration

If you want to use Neon as a database service then follow the steps below:

1. Sign up at [Neon](https://neon.tech) and create a new project.
2. Click the `Connect` button and copy the PostgreSQL
  connection URL.
3. Paste that URL into your deployment or local `.env` file as `DATABASE_URL`.

### OpenAI configuration

Configure the OpenAI-compatible LLM settings in your deployment or local `.env`
file:

- `OPENAI_BASE_URL` — base URL for the LLM API
- `OPENAI_API_KEY` — your API key for the LLM provider.

If you would like to use OpenCode Zen,
[create an API key here.](https://opencode.ai/auth)

### WebAuthn & OAuth configuration

This project includes WebAuthn and OAuth flows used by front-end application and
MCP clients. If you want to use the front-end too,
[see this repository](https://github.com/MiguelRipoll23/pasta)

To use these features, set the following environment variables:

- `OAUTH_APP_BASE_URL` — the front-end application's base URL used for OAuth
  redirects (e.g. `https://your-pasta-app.vercel.app`).
- `WEBAUTHN_ORIGINS` — a comma-separated list of allowed origins for WebAuthn
  (e.g. `https://your-pasta-app.vercel.app`).

## Running the server

Start the server in development mode:

```bash
deno task dev
```

The MCP servers are available at:

- `/api/v1/mcp/global`
- `/api/v1/mcp/portfolio`
- `/api/v1/mcp/expenses`
