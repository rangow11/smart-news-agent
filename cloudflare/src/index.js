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

      if (url.pathname === "/api/reports" && request.method === "GET") {
        const category = url.searchParams.get("category") || "";
        const status = url.searchParams.get("status") || "";
        const q = (url.searchParams.get("q") || "").slice(0, 160);
        const minScore = Math.max(0, Math.min(100, Number(url.searchParams.get("min_score") || 0)));
        const limit = Math.max(1, Math.min(100, Number(url.searchParams.get("limit") || 50)));
        const offset = Math.max(0, Number(url.searchParams.get("offset") || 0));
        const rows = await sql`
          SELECT id, source_name, title, url, published_at, fetched_at, content,
                 category, summary, importance, mission_relevance, freshness,
                 source_quality, total_score, is_rumor, ai_model, review_status,
                 review_note, reviewed_at
          FROM news_reports
          WHERE (NULLIF(${category}, '') IS NULL OR category = ${category})
            AND (NULLIF(${status}, '') IS NULL OR review_status = ${status})
            AND total_score >= ${minScore}
            AND (NULLIF(${q}, '') IS NULL OR title ILIKE '%' || ${q} || '%'
                 OR summary ILIKE '%' || ${q} || '%' OR source_name ILIKE '%' || ${q} || '%')
          ORDER BY fetched_at DESC
          LIMIT ${limit} OFFSET ${offset}
        `;
        return json({ reports: rows, count: rows.length, limit, offset, statuses: STATUSES });
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
