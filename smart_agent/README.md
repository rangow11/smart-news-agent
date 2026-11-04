# Smart News Agent cloud worker (MVP)

This worker is intentionally separate from the legacy Topic Watch application.
It creates its own PostgreSQL tables in the existing Neon database and does not
modify or migrate the legacy SQLite tables.

## GitHub setup

In **Settings → Secrets and variables → Actions**, add these repository secrets:

- `NEON_DATABASE_URL`: Neon PostgreSQL connection string (keep it private).
- `GEMINI_API_KEY`: Google AI Studio API key.
- `GROK_API_KEY`: xAI API key (used only as fallback).

Add these repository variables:

- `NEWS_SOURCES_JSON`: JSON array of public RSS/Atom sources, for example:

```json
[
  {"name": "Example News", "feed_url": "https://example.com/rss.xml", "source_quality": 80},
  {"name": "Example World", "feed_url": "https://example.org/feed"}
]
```

Optional variables:

- `GEMINI_MODEL` (default: `gemini-2.5-flash`)
- `GROK_MODEL` (default: `grok-3-mini`)

The scheduled workflow runs every 15 minutes. It can also be started manually
from the Actions tab once this workflow exists on the default branch.

## Current behavior

1. Reads enabled public RSS/Atom sources from `NEWS_SOURCES_JSON`.
2. Stores source and report records in Neon PostgreSQL.
3. Deduplicates by canonical URL (tracking query strings and fragments are ignored).
4. Sends each new report to Gemini first.
5. Uses Grok only when Gemini responds with HTTP 429 or a temporary 5xx outage.
6. Stores the provider, model, source attribution, summary, category and weighted
   score with each report.

Scoring weights: importance 40%, mission relevance 30%, freshness 20%, source
quality 10%. This is a relevance/priority score, **not a verification score**.
Claims remain attributed to their source; rumor-like content is labeled.

## Important MVP limitations

- Source lists are currently configured through the GitHub repository variable;
  the interactive source-management dashboard is a later step.
- The dashboard/event aggregation, ntfy alerts, and organization review workflow
  are not wired to this worker yet.
- The worker processes RSS-provided summaries; full article extraction and richer
  event clustering are later steps.
- Do not put confidential or operationally sensitive information in public feeds
  or send it to external AI providers.
- The scheduled workflow starts automatically only after the workflow is present
  on the repository's default branch. Until then, use a manual workflow run from
  a branch where GitHub makes it available, or merge the reviewed PR first.
