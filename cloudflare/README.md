# Cloudflare dashboard (MVP)

This is a mobile-friendly Persian/RTL review dashboard backed by the Neon
`news_reports` table. It lists individual reports (event aggregation is not
implemented yet) and lets an operator update the review status.

## Deploy

1. Create a Cloudflare Worker project connected to this repository, with the
   working directory set to `cloudflare`.
2. Build/deploy with `npm install` and `npm run deploy`.
3. Set Worker secrets:
   - `DATABASE_URL`: the Neon connection string.
   - `DASHBOARD_ACCESS_TOKEN`: a long random token (at least 24 characters).
4. Open the Worker URL. Enter the access token in the dashboard login panel.

For local preview, run `npm install` then `npm run dev` from this directory and
configure the same two secrets in Wrangler local secrets.

## API

- `GET /api/health`
- `GET /api/reports?category=&status=&q=&min_score=65&limit=50&offset=0`
- `PATCH /api/reports/:id/status` with JSON `{"status":"انتخاب‌شده"}`

Allowed review statuses: جدید، انتخاب‌شده، ارسال‌شده، منتشرشده، ردشده،
ذخیره‌شده برای بررسی.

The API is protected by a bearer token; do not expose the Neon connection
string in browser code. Only public, non-sensitive news should be stored here.
