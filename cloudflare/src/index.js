import { neon } from "@neondatabase/serverless";

const STATUSES = ["جدید", "انتخاب‌شده", "ارسال‌شده", "منتشرشده", "ردشده", "ذخیره‌شده برای بررسی"];
const CORS_HEADERS = {
  "Access-Control-Allow-Methods": "GET, POST, PATCH, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Cache-Control": "no-store",
};

function tehranClock(timestamp) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Tehran",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date(timestamp));
  const get = (type) => Number(parts.find((part) => part.type === type)?.value || 0);
  return { hour: get("hour"), minute: get("minute") };
}

async function shouldDispatchCollection(timestamp, env) {
  const { hour, minute } = tehranClock(timestamp);
  if (hour < 6 || hour >= 23) return false;

  let interval = 30;
  if (env.DATABASE_URL) {
    const sql = neon(env.DATABASE_URL);
    const rows = await sql`SELECT setting_value FROM agent_settings WHERE setting_key = 'run_interval_minutes'`;
    interval = Number(rows[0]?.setting_value || 30);
  }

  // 30 means the requested smart schedule. Any other saved value activates
  // a temporary interval override (useful for 5-minute smoke tests).
  if (interval !== 30) {
    const minutesSince0600 = (hour - 6) * 60 + minute;
    return minutesSince0600 % interval === 0;
  }

  // Smart schedule: 06:00-14:00 every 30 minutes; 14:00-23:00 hourly.
  if (minute !== 0 && minute !== 30) return false;
  if (hour >= 14 && minute !== 0) return false;
  return true;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...CORS_HEADERS },
  });
}


function validFeedUrl(value) {
  try {
    const u = new URL(value);
    if (u.protocol !== "https:" || u.username || u.password) return false;
    const h = u.hostname.toLowerCase();
    if ((h === "t.me" || h === "telegram.me") && /^\/s\/[A-Za-z0-9_]{4,}$/.test(u.pathname)) return true;
    if (h === "localhost" || h.endsWith(".localhost") || h.endsWith(".local") ||
        h === "metadata.google.internal" || h === "::1" || h === "[::1]" || h.startsWith("fc") || h.startsWith("fe80:")) return false;
    if (/^(10\.|127\.|169\.254\.|192\.168\.)/.test(h)) return false;
    const m = h.match(/^172\.(\d+)\./);
    if (m && Number(m[1]) >= 16 && Number(m[1]) <= 31) return false;
    return true;
  } catch { return false; }
}

async function ensureSourceSchema(sql) {
  await sql`CREATE TABLE IF NOT EXISTS news_sources (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, feed_url TEXT NOT NULL UNIQUE, enabled BOOLEAN NOT NULL DEFAULT TRUE, source_quality SMALLINT NOT NULL DEFAULT 50, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'unknown'`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS region TEXT NOT NULL DEFAULT ''`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS last_report_at TIMESTAMPTZ`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS last_error TEXT`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS operator_enabled BOOLEAN`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS operator_quality_override SMALLINT`;
  await sql`ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS telegram_parser_version SMALLINT NOT NULL DEFAULT 0`;
}
async function ensureNotificationSchema(sql) {
  await sql`CREATE TABLE IF NOT EXISTS agent_settings (
    setting_key TEXT PRIMARY KEY, setting_value TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`;
}
async function verifyFeedUrl(value) {
  if (!validFeedUrl(value)) return { ok: false, error: "feed_url_must_be_public_https" };
  try {
    const response = await fetch(value, {
      method: "GET",
      redirect: "manual",
      headers: { "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*" },
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) return { ok: false, error: "feed_http_" + response.status };
    const type = response.headers.get("content-type") || "";
    const body = (await response.text()).slice(0, 1000000);
    const checkedUrl = new URL(value);
    if ((checkedUrl.hostname === "t.me" || checkedUrl.hostname === "telegram.me") &&
        body.includes("tgme_widget_message_wrap")) return { ok: true, content_type: type };
    if (!/(<rss[\s>]|<feed[\s>]|<rdf:RDF[\s>])/i.test(body)) {
      return { ok: false, error: "not_valid_rss_or_atom" };
    }
    return { ok: true, content_type: type };
  } catch (error) {
    return { ok: false, error: "feed_check_failed" };
  }
}

export default {
  async scheduled(controller, env, ctx) {
    const scheduledAt = Number.isFinite(controller?.scheduledTime) ? controller.scheduledTime : Date.now();
    const local = tehranClock(scheduledAt);
    if (!(await shouldDispatchCollection(scheduledAt, env))) {
      console.log(`Scheduled collection skipped at Tehran ${String(local.hour).padStart(2, "0")}:${String(local.minute).padStart(2, "0")}`);
      return;
    }
    console.log(`Scheduled collection allowed at Tehran ${String(local.hour).padStart(2, "0")}:${String(local.minute).padStart(2, "0")}`);
    if (!env.GITHUB_ACTIONS_TOKEN) {
      console.error("GITHUB_ACTIONS_TOKEN is not configured; scheduled dispatch skipped");
      return;
    }
    const response = await fetch("https://api.github.com/repos/rangow11/smart-news-agent/actions/workflows/smart-news-agent.yml/dispatches", {
      method: "POST",
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${env.GITHUB_ACTIONS_TOKEN}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "smart-news-agent-cloudflare-cron"
      },
      body: JSON.stringify({ ref: "main" })
    });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(`GitHub workflow dispatch failed: HTTP ${response.status} ${body.slice(0, 500)}`);
    }
    console.log("Smart News Agent workflow dispatched from Cloudflare Cron");
  },
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return new Response(null, { headers: CORS_HEADERS });
    if (!url.pathname.startsWith("/api/")) return env.ASSETS.fetch(request);
    if (!env.DATABASE_URL) return json({ error: "database_not_configured" }, 503);

    try {
      const sql = neon(env.DATABASE_URL);
      if (url.pathname === "/api/health" && request.method === "GET") {
        const rows = await sql`SELECT NOW() AS database_time`;
        return json({ ok: true, database_time: rows[0].database_time });
      }


      if (url.pathname === "/api/sources" && request.method === "GET") {
        await ensureSourceSchema(sql);
        const sources = await sql`
          SELECT id, name, feed_url, COALESCE(operator_enabled, enabled) AS enabled,
                 COALESCE(operator_quality_override, source_quality) AS source_quality,
                 language, region, last_checked_at, last_success_at, last_report_at, last_error
          FROM news_sources ORDER BY name
        `;
        return json({ sources });
      }

      if (url.pathname === "/api/sources" && request.method === "POST") {
        await ensureSourceSchema(sql);
        const body = await request.json();
        const name = typeof body.name === "string" ? body.name.trim().slice(0, 120) : "";
        const feedUrl = typeof body.feed_url === "string" ? body.feed_url.trim() : "";
        const language = ["fa", "en", "ar", "he", "other"].includes(body.language) ? body.language : "other";
        const region = typeof body.region === "string" ? body.region.trim().slice(0, 80) : "";
        const quality = Number(body.source_quality);
        if (!name || !validFeedUrl(feedUrl) || !Number.isInteger(quality) || quality < 0 || quality > 100) {
          return json({ error: "invalid_source_fields" }, 400);
        }
        const check = await verifyFeedUrl(feedUrl);
        if (!check.ok) return json({ error: check.error }, 422);
        const rows = await sql`
          INSERT INTO news_sources (name, feed_url, enabled, source_quality, language, region)
          VALUES (${name}, ${feedUrl}, TRUE, ${quality}, ${language}, ${region})
          ON CONFLICT (feed_url) DO UPDATE
          SET name = EXCLUDED.name, language = EXCLUDED.language, region = EXCLUDED.region,
              source_quality = EXCLUDED.source_quality
          RETURNING id, name, feed_url, enabled, source_quality, language, region
        `;
        return json({ source: rows[0] }, 201);
      }

      if (url.pathname === "/api/notifications" && request.method === "GET") {
        await ensureNotificationSchema(sql);
        const rows = await sql`SELECT setting_value FROM agent_settings WHERE setting_key = 'ntfy_topic'`;
        return json({ configured: Boolean(rows[0]?.setting_value), topic: rows[0]?.setting_value || null });
      }
      if (url.pathname === "/api/notifications" && request.method === "POST") {
        await ensureNotificationSchema(sql);
        const body = await request.json();
        const topic = typeof body.topic === "string" ? body.topic.trim() : "";
        if (!/^[A-Za-z0-9_-]{8,64}$/.test(topic)) return json({ error: "invalid_notification_topic" }, 400);
        await sql`
          INSERT INTO agent_settings (setting_key, setting_value)
          VALUES ('ntfy_topic', ${topic})
          ON CONFLICT (setting_key) DO UPDATE
          SET setting_value = EXCLUDED.setting_value, updated_at = NOW()
        `;
        return json({ configured: true, topic });
      }

      if (url.pathname === "/api/settings" && request.method === "GET") {
        await ensureNotificationSchema(sql);
        const rows = await sql`SELECT setting_value FROM agent_settings WHERE setting_key = 'run_interval_minutes'`;
        const lastRun = await sql`SELECT state_value FROM agent_state WHERE state_key = 'last_worker_run'`;
        return json({
          run_interval_minutes: Number(rows[0]?.setting_value || 30),
          last_run_at: lastRun[0]?.state_value || null,
          scheduler_tick_minutes: 5
        });
      }
      if (url.pathname === "/api/settings" && request.method === "POST") {
        await ensureNotificationSchema(sql);
        const body = await request.json();
        const interval = Number(body.run_interval_minutes);
        if (!Number.isInteger(interval) || interval < 5 || interval > 1440 || interval % 5 !== 0) {
          return json({ error: "invalid_run_interval", message: "فاصله باید مضربی از ۵ و بین ۵ تا ۱۴۴۰ دقیقه باشد." }, 400);
        }
        await sql`
          INSERT INTO agent_settings (setting_key, setting_value)
          VALUES ('run_interval_minutes', ${String(interval)})
          ON CONFLICT (setting_key) DO UPDATE
          SET setting_value = EXCLUDED.setting_value, updated_at = NOW()
        `;
        return json({ run_interval_minutes: interval, scheduler_tick_minutes: 5, saved: true });
      }

      const sourceMatch = url.pathname.match(/^\/api\/sources\/(\d+)$/);
      if (sourceMatch && request.method === "PATCH") {
        await ensureSourceSchema(sql);
        const id = Number(sourceMatch[1]);
        const body = await request.json();
        const name = typeof body.name === "string" ? body.name.trim().slice(0, 120) : null;
        const language = ["fa", "en", "ar", "he", "other"].includes(body.language) ? body.language : null;
        const region = typeof body.region === "string" ? body.region.trim().slice(0, 80) : null;
        const enabled = typeof body.enabled === "boolean" ? body.enabled : null;
        const quality = Number.isInteger(body.source_quality) && body.source_quality >= 0 && body.source_quality <= 100
          ? body.source_quality : null;
        const feedUrl = typeof body.feed_url === "string" ? body.feed_url.trim() : null;
        if (feedUrl && !validFeedUrl(feedUrl)) return json({ error: "invalid_feed_url" }, 400);
        if (feedUrl) {
          const check = await verifyFeedUrl(feedUrl);
          if (!check.ok) return json({ error: check.error }, 422);
        }
        const rows = await sql`
          UPDATE news_sources SET
            name = COALESCE(${name}, name),
            language = COALESCE(${language}, language),
            region = COALESCE(${region}, region),
            operator_enabled = COALESCE(${enabled}, operator_enabled),
            operator_quality_override = COALESCE(${quality}, operator_quality_override),
            feed_url = COALESCE(${feedUrl}, feed_url)
          WHERE id = ${id}
          RETURNING id, name, feed_url, COALESCE(operator_enabled, enabled) AS enabled,
                    COALESCE(operator_quality_override, source_quality) AS source_quality,
                    language, region
        `;
        if (!rows.length) return json({ error: "source_not_found" }, 404);
        return json({ source: rows[0] });
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

      if (url.pathname === "/api/diagnostics/reports-schema" && request.method === "GET") {
        const requiredColumns = [
          "id", "source_name", "title", "url", "published_at", "fetched_at", "content",
          "category", "news_track", "summary", "detailed_report", "importance",
          "mission_relevance", "freshness", "source_quality", "total_score",
          "is_rumor", "ai_model", "review_status", "review_note", "reviewed_at"
        ];
        const columns = await sql`
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_schema = current_schema() AND table_name = 'news_reports'
        `;
        const available = new Map(columns.map((column) => [column.column_name, column.data_type]));
        const missing = requiredColumns.filter((name) => !available.has(name));
        return json({
          table_exists: columns.length > 0,
          required_columns: requiredColumns.map((name) => ({
            name, exists: available.has(name), type: available.get(name) || null
          })),
          missing_columns: missing
        });
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
        const hourSlotRaw = url.searchParams.get("hour_slot") || "";
        const hourSlot = /^([0-9]|1[0-9]|2[0-3])$/.test(hourSlotRaw) ? Number(hourSlotRaw) : null;
        const sort = ["score", "newest", "combined"].includes(url.searchParams.get("sort"))
          ? url.searchParams.get("sort") : "combined";
        const requestedLimit = Number(url.searchParams.get("limit") || 50);
        const limit = Number.isFinite(requestedLimit) ? Math.max(1, Math.min(100, requestedLimit)) : 50;
        const requestedOffset = Number(url.searchParams.get("offset") || 0);
        const offset = Number.isFinite(requestedOffset) ? Math.max(0, requestedOffset) : 0;
        const rows = await sql`
          SELECT id, source_name, title, url, published_at, fetched_at, content,
                 category, news_track, summary, detailed_report, importance, mission_relevance, freshness,
                 source_quality, total_score, is_rumor, ai_model, review_status,
                 review_note, reviewed_at
          FROM news_reports
          WHERE (NULLIF(${category}, '') IS NULL OR category = ${category})
            AND (NULLIF(${status}, '') IS NULL OR review_status = ${status})
            AND (NULLIF(${source}, '') IS NULL OR source_name = ${source})
            AND total_score >= ${minScore} AND total_score <= ${maxScore}
            AND (NULLIF(${dateFrom}, '') IS NULL OR published_at >= NULLIF(${dateFrom}, '')::date)
            AND (NULLIF(${dateTo}, '') IS NULL OR published_at < (NULLIF(${dateTo}, '')::date + INTERVAL '1 day'))
            AND (${hourSlot}::int IS NULL OR EXTRACT(HOUR FROM published_at AT TIME ZONE 'Asia/Tehran') = ${hourSlot})
            AND (NULLIF(${q}, '') IS NULL OR title ILIKE '%' || ${q} || '%'
                 OR summary ILIKE '%' || ${q} || '%' OR source_name ILIKE '%' || ${q} || '%')
          ORDER BY
            CASE WHEN ${sort} = 'newest' THEN published_at END DESC NULLS LAST,
            CASE WHEN ${sort} = 'score' THEN total_score END DESC NULLS LAST,
            total_score DESC NULLS LAST, published_at DESC NULLS LAST,
            CASE WHEN ${sort} = 'combined' THEN (SELECT COUNT(*) FROM news_reports sent WHERE sent.source_name = news_reports.source_name AND sent.review_status IN ('ارسال‌شده','منتشرشده')) END ASC NULLS LAST,
            fetched_at DESC
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
