import { neon } from "@neondatabase/serverless";

const STATUSES = ["جدید", "انتخاب‌شده", "ارسال‌شده", "منتشرشده", "ردشده", "ذخیره‌شده برای بررسی"];
const CORS_HEADERS = {
  "Access-Control-Allow-Methods": "GET, PATCH, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Cache-Control": "no-store",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...CORS_HEADERS },
  });
}

function authorized(request, env) {
  const expected = env.DASHBOARD_ACCESS_TOKEN || "";
  const supplied = request.headers.get("Authorization") || "";
  return expected.length >= 24 && supplied === `Bearer ${expected}`;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return new Response(null, { headers: CORS_HEADERS });
    if (!url.pathname.startsWith("/api/")) return env.ASSETS.fetch(request);
    if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
    if (!env.DATABASE_URL) return json({ error: "database_not_configured" }, 503);

    try {
      const sql = neon(env.DATABASE_URL);
      if (url.pathname === "/api/health" && request.method === "GET") {
        const rows = await sql`SELECT NOW() AS database_time`;
        return json({ ok: true, database_time: rows[0].database_time });
      }

      if (url.pathname === "/api/stats" && request.method === "GET") {
        const totals = await sql`
          SELECT COUNT(*)::int AS total,
                 COUNT(*) FILTER (WHERE fetched_at >= NOW() - INTERVAL '24 hours')::int AS last_24h,
                 COUNT(*) FILTER (WHERE total_score >= 65)::int AS priority_65,
                 COUNT(*) FILTER (WHERE total_score >= 85)::int AS priority_85,
                 COUNT(*) FILTER (WHERE is_rumor)::int AS rumors,
                 MAX(fetched_at) AS latest_report
          FROM news_reports
        `;
        const categories = await sql`
          SELECT category, COUNT(*)::int AS count
          FROM news_reports GROUP BY category ORDER BY count DESC, category
        `;
        const statuses = await sql`
          SELECT review_status AS status, COUNT(*)::int AS count
          FROM news_reports GROUP BY review_status ORDER BY count DESC, status
        `;
        const checkpoint = await sql`
          SELECT state_value FROM agent_state WHERE state_key = 'last_successful_run'
        `;
        return json({
          totals: totals[0],
          categories,
          statuses,
          last_successful_run: checkpoint[0]?.state_value || null,
        });
      }
      if (url.pathname === "/api/filters" && request.method === "GET") {
        const sources = await sql`SELECT DISTINCT source_name FROM news_reports ORDER BY source_name`;
        return json({ sources: sources.map((row) => row.source_name) });
      }

      if (url.pathname === "/api/reports" && request.method === "GET") {
        const category = url.searchParams.get("category") || "";
        const status = url.searchParams.get("status") || "";
        const q = (url.searchParams.get("q") || "").slice(0, 160);
        const minScore = Math.max(0, Math.min(100, Number(url.searchParams.get("min_score") || 0)));
        const maxScoreRaw = url.searchParams.get("max_score");
        const maxScore = maxScoreRaw === null || maxScoreRaw === "" ? 100 : Math.max(0, Math.min(100, Number(maxScoreRaw)));
        const source = url.searchParams.get("source") || "";
        const dateFrom = url.searchParams.get("date_from") || "";
        const dateTo = url.searchParams.get("date_to") || "";
        const sort = ["score", "newest", "combined"].includes(url.searchParams.get("sort"))
          ? url.searchParams.get("sort") : "combined";
        const requestedLimit = Number(url.searchParams.get("limit") || 50);
        const limit = Number.isFinite(requestedLimit) ? Math.max(1, Math.min(100, requestedLimit)) : 50;
        const requestedOffset = Number(url.searchParams.get("offset") || 0);
        const offset = Number.isFinite(requestedOffset) ? Math.max(0, requestedOffset) : 0;
        const rows = await sql`
          SELECT id, source_name, title, url, published_at, fetched_at, content,
                 category, news_track, summary, importance, mission_relevance, freshness,
                 source_quality, total_score, is_rumor, ai_model, review_status,
                 review_note, reviewed_at
          FROM news_reports
          WHERE (NULLIF(${category}, '') IS NULL OR category = ${category})
            AND (NULLIF(${status}, '') IS NULL OR review_status = ${status})
            AND (NULLIF(${source}, '') IS NULL OR source_name = ${source})
            AND total_score >= ${minScore} AND total_score <= ${maxScore}
            AND (NULLIF(${dateFrom}, '') IS NULL OR published_at >= ${dateFrom}::date)
            AND (NULLIF(${dateTo}, '') IS NULL OR published_at < (${dateTo}::date + INTERVAL '1 day'))
            AND (NULLIF(${q}, '') IS NULL OR title ILIKE '%' || ${q} || '%'
                 OR summary ILIKE '%' || ${q} || '%' OR source_name ILIKE '%' || ${q} || '%')
          ORDER BY
            CASE WHEN ${sort} = 'newest' THEN published_at END DESC NULLS LAST,
            CASE WHEN ${sort} = 'score' THEN total_score END DESC NULLS LAST,
            CASE WHEN ${sort} = 'combined' THEN (SELECT COUNT(*) FROM news_reports sent WHERE sent.source_name = news_reports.source_name AND sent.review_status IN ('ارسال‌شده','منتشرشده')) END ASC NULLS LAST,
            total_score DESC NULLS LAST, published_at DESC NULLS LAST, fetched_at DESC
          LIMIT ${limit + 1} OFFSET ${offset}
        `;
        const hasMore = rows.length > limit;
        const reports = hasMore ? rows.slice(0, limit) : rows;
        return json({ reports, count: reports.length, limit, offset, has_more: hasMore, statuses: STATUSES });
      }

      const match = url.pathname.match(/^\/api\/reports\/(\d+)\/status$/);
      if (match && request.method === "PATCH") {
        const id = Number(match[1]);
        const body = await request.json();
        if (!STATUSES.includes(body.status)) return json({ error: "invalid_status" }, 400);
        const note = typeof body.note === "string" ? body.note.slice(0, 1000) : null;
        const rows = await sql`
          UPDATE news_reports
          SET review_status = ${body.status}, review_note = ${note}, reviewed_at = NOW()
          WHERE id = ${id}
          RETURNING id, review_status, review_note, reviewed_at
        `;
        if (!rows.length) return json({ error: "not_found" }, 404);
        return json({ report: rows[0] });
      }

      return json({ error: "not_found" }, 404);
    } catch (error) {
      console.error("dashboard_api_error", error?.message || "unknown");
      return json({ error: "internal_error" }, 500);
    }
  },
};
