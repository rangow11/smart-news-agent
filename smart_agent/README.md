# Smart News Agent cloud worker (MVP)

This worker is separate from the legacy Topic Watch application. It creates its
own PostgreSQL tables in Neon and leaves the legacy SQLite database untouched.

## 1. GitHub configuration

Open **Repository → Settings → Secrets and variables → Actions**.

Create these repository **Secrets** (never paste their values into chat or commit them):

- `NEON_DATABASE_URL`: Neon PostgreSQL connection string.
- `GEMINI_API_KEY`: Google AI Studio API key.

Create these repository **Variables**:

- `NEWS_SOURCES_JSON`: JSON array of public RSS/Atom feeds.
- `GEMINI_MODEL`: optional; defaults to `gemini-3.8-flash`.
- `GEMINI_FALLBACK_MODELS`: optional comma-separated model IDs. If omitted, the
  worker tries `gemini-3.7-flash`, `gemini-3.6-flash`, `gemini-3.5-flash`,
  `gemini-3.5-flash-lite`, and `gemini-3.1-flash-lite` in that order.

Example value for `NEWS_SOURCES_JSON` (replace with the feeds you want to monitor):

```json
[
  {"name": "Example News", "feed_url": "https://example.com/rss.xml", "source_quality": 80},
  {"name": "Example World", "feed_url": "https://example.org/feed", "source_quality": 70}
]
```

The source quality is an operator-assigned value from 0 to 100; it is not
estimated by Gemini. Only use public, trusted RSS/Atom endpoints.

## 2. Run

The workflow is `.github/workflows/smart-news-agent.yml`. It can be started
manually from the GitHub Actions tab when available. It is configured to run
every 15 minutes after it is merged into the repository's default branch
(`main`). GitHub scheduled workflows do not run from feature branches.

## 3. What it does

1. Connects to Neon and creates the worker's own `news_sources` and
   `news_reports` tables.
2. Reads the configured public RSS/Atom feeds.
3. Deduplicates reports by canonical URL before calling the model.
4. Sends each new report to Gemini and falls back through the configured
   Gemini model ladder on quota/rate-limit, temporary service/network errors,
   or unusable model output.
5. Validates the JSON structure and category before saving the report.
6. Stores the original source attribution, link, summary, category, scores,
   rumor flag, model name and processing timestamp.

Scoring weights: importance 40%, mission relevance 30%, freshness 20%, source
quality 10%. This is a priority score, **not a truth/verification score**.
Claims remain attributed to their source; rumor-like content is labeled.
The worker does not invent news and does not automatically publish anything.

## 4. Current MVP boundaries

- This branch contains the ingestion and analysis worker; the user-facing
  dashboard, event clustering, ntfy notifications and review/publishing flow
  are not connected yet.
- The worker processes RSS-provided summaries/descriptions, not full-page
  extraction.
- Only public, non-sensitive information should be sent to external AI APIs.
- Keep API keys and the Neon connection string in GitHub Secrets only.
