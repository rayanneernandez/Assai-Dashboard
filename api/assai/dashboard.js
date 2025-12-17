// api/assai/dashboard.js
import { Pool } from "pg";

// ===============================
// Postgres
// ===============================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: parseInt(process.env.PG_POOL_MAX || "10", 10),
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  keepAlive: true,
});

async function q(sql, params) {
  for (let i = 0; i < 3; i++) {
    try {
      return await pool.query(sql, params);
    } catch (e) {
      const msg = String(e?.message || "");
      if (/Connection terminated unexpectedly|ECONNRESET|ETIMEDOUT/i.test(msg)) {
        await new Promise((r) => setTimeout(r, 200 * (i + 1)));
        continue;
      }
      throw e;
    }
  }
  return await pool.query(sql, params);
}

// ===============================
// DisplayForce
// ===============================
// ⚠️ NÃO deixe token hardcoded em produção.
// Deixe apenas no ENV (DISPLAYFORCE_API_TOKEN).
const DISPLAYFORCE_TOKEN = process.env.DISPLAYFORCE_API_TOKEN || "";
const DISPLAYFORCE_BASE =
  process.env.DISPLAYFORCE_API_URL || "https://api.displayforce.ai/public/v1";

// Timeouts (pra não “carregar infinito”)
const FETCH_TIMEOUT_MS = parseInt(process.env.FETCH_TIMEOUT_MS || "12000", 10);
async function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    return resp;
  } finally {
    clearTimeout(id);
  }
}

// Cache simples em memória (serverless = best-effort)
const SUMMARY_CACHE = new Map();
function cacheKey(sDate, eDate, storeId) {
  return `${sDate}|${eDate}|${storeId || "all"}`;
}

// ===============================
// Ajustes de horário
// ===============================
// TIMEZONE_OFFSET_HOURS = offset do seu dado para gerar local_time (ex.: -3 São Paulo)
// CHART_HOUR_SHIFT = ajuste apenas de EXIBIÇÃO nos gráficos (ex.: +3)
const TIMEZONE_OFFSET_HOURS = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
const CHART_HOUR_SHIFT = parseInt(process.env.CHART_HOUR_SHIFT || "3", 10);

function shiftHourlyBuckets(byHour, byGenderHour, offsetHours) {
  const outByHour = {};
  const outByGenderHour = { male: {}, female: {} };

  for (let h = 0; h < 24; h++) {
    outByHour[h] = 0;
    outByGenderHour.male[h] = 0;
    outByGenderHour.female[h] = 0;
  }

  for (let h = 0; h < 24; h++) {
    const newH = (h + offsetHours + 24) % 24;
    outByHour[newH] += Number(byHour?.[h] || 0);
    outByGenderHour.male[newH] += Number(byGenderHour?.male?.[h] || 0);
    outByGenderHour.female[newH] += Number(byGenderHour?.female?.[h] || 0);
  }

  return { byHour: outByHour, byGenderHour: outByGenderHour };
}

function isoDay(d = new Date()) {
  return new Date(d).toISOString().slice(0, 10);
}

function tzISO(day, hhmmss) {
  const tz = TIMEZONE_OFFSET_HOURS;
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  return `${day}T${hhmmss}${tzStr}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ===============================
// Handler
// ===============================
export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Cache-Control", "no-store, max-age=0, s-maxage=0");

  if (req.method === "OPTIONS") return res.status(200).end();

  const { endpoint, start_date, end_date, store_id } = req.query;
  const ep = String(endpoint || "").trim().toLowerCase();

  try {
    console.log(
      `📊 Endpoint=${ep} start=${start_date} end=${end_date} store=${store_id}`
    );

    switch (ep) {
      case "visitors":
        return await getVisitors(req, res, start_date, end_date, store_id);

      case "summary":
        return await getSummary(req, res, start_date, end_date, store_id);

      case "stores":
        return await getStores(req, res);

      case "devices":
        return await getDevices(req, res);

      case "refresh":
        return await refreshRange(req, res, start_date, end_date, store_id);

      case "refresh_all":
        return await refreshAll(req, res, start_date, end_date);

      case "sync_all_data":
        return await syncAllHistoricalData(req, res);

      case "plan_ingest":
        return await planIngestDay(req, res, start_date, end_date, store_id);

      case "ingest_day":
        return await ingestDay(req, res, start_date, end_date, store_id);

      case "auto_refresh":
        return await autoRefresh(req, res);

      case "force_sync_today":
      case "sync_today":
        return await forceSyncToday(req, res);

      case "wipe_range":
        return await wipeRange(req, res, start_date, end_date);

      case "verify_day":
        return await verifyDay(req, res, start_date, store_id);

      case "rebuild_hourly":
        return await rebuildHourlyFromVisitors(req, res, start_date, end_date, store_id);

      case "refresh_recent":
      case "recent":
        return await refreshRecent(req, res, start_date, store_id);

      case "optimize":
        return await ensureIndexes(req, res);

      case "backfill_local_time":
        return await backfillLocalTime(req, res);

      case "test":
        return res.status(200).json({
          success: true,
          message: "API Assaí está funcionando!",
          endpoints: [
            "visitors",
            "summary",
            "stores",
            "devices",
            "refresh",
            "refresh_all",
            "sync_all_data",
            "auto_refresh",
            "optimize",
            "test",
          ],
          timestamp: new Date().toISOString(),
        });

      default:
        return res.status(200).json({
          success: true,
          message: "API Assaí Dashboard",
          usage: "Use ?endpoint=summary&start_date=2025-12-01&end_date=2025-12-02",
          available_endpoints: [
            "visitors",
            "summary",
            "stores",
            "devices",
            "refresh",
            "refresh_all",
            "sync_all_data",
            "auto_refresh",
            "optimize",
            "test",
          ],
        });
    }
  } catch (error) {
    console.error("🔥 API Error:", error);
    return res.status(500).json({
      success: false,
      error: "Internal server error",
      message: error?.message || String(error),
    });
  }
}

// ===============================
// 1) SYNC ALL HISTÓRICO
// ===============================
async function syncAllHistoricalData(req, res) {
  try {
    console.log("🚀 SYNC ALL HISTÓRICO INICIADO");

    const devices = await fetchDisplayForceDevices();
    console.log(`📱 ${devices.length} dispositivos`);

    const results = [];
    for (const device of devices) {
      try {
        console.log(`🔄 Sync device ${device.id}`);
        const visitors = await fetchAllVisitorsFromDisplayForce(device.id);
        const saved = await saveVisitorsToDatabase(visitors, undefined, String(req.query.mode || ""));

        // Atualiza agregados de todas as datas do device (e depois all)
        await updateAllAggregatesForDevice(device.id);

        results.push({
          device_id: device.id,
          visitors_found: visitors.length,
          visitors_saved: saved,
          success: true,
        });
      } catch (e) {
        results.push({ device_id: device.id, success: false, error: e?.message || String(e) });
      }
    }

    await updateAllAggregatesForDevice("all");

    return res.status(200).json({
      success: true,
      message: "Sincronização completa concluída",
      results,
      total_devices: devices.length,
      timestamp: new Date().toISOString(),
    });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

async function fetchAllVisitorsFromDisplayForce(device_id = null) {
  if (!DISPLAYFORCE_TOKEN) throw new Error("DISPLAYFORCE_API_TOKEN não configurado");

  const LIMIT = 500;
  let offset = 0;
  const allVisitors = [];
  let totalProcessed = 0;
  let totalFromAPI = 0;

  console.log(`🔍 Buscando TODOS os visitantes ${device_id ? `device=${device_id}` : ""}`);

  while (true) {
    const bodyPayload = {
      limit: LIMIT,
      offset,
      tracks: true,
      face_quality: true,
      glasses: true,
      facial_hair: true,
      hair_color: true,
      hair_type: true,
      headwear: true,
    };
    if (device_id) bodyPayload.devices = [parseInt(device_id, 10)];

    const response = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: "POST",
      headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify(bodyPayload),
    });

    if (!response.ok) {
      const t = await response.text().catch(() => "");
      throw new Error(`DisplayForce Error ${response.status}: ${t}`);
    }

    const data = await response.json();
    const visitors = data.payload || data || [];

    allVisitors.push(...visitors);
    totalProcessed += visitors.length;

    if (data.pagination) {
      totalFromAPI = Number(data.pagination.total || 0);
      if (totalFromAPI && totalProcessed >= totalFromAPI) break;
    }

    if (visitors.length < LIMIT) break;

    offset += LIMIT;
    if (offset >= 100000) {
      console.warn("⚠️ Limite de segurança atingido (100k offset)");
      break;
    }

    await sleep(200);
  }

  console.log(`✅ Total final: ${allVisitors.length}`);
  return allVisitors;
}

async function updateAllAggregatesForDevice(device_id) {
  try {
    console.log(`📈 Atualizando agregados para ${device_id}...`);

    let query = `SELECT DISTINCT day FROM visitors WHERE 1=1`;
    const params = [];

    if (device_id !== "all") {
      query += ` AND store_id = $1`;
      params.push(device_id);
    }

    query += ` ORDER BY day`;

    const result = await pool.query(query, params);
    const uniqueDates = result.rows.map((r) => String(r.day));

    for (const day of uniqueDates) {
      await upsertAggregatesForDate(day); // já atualiza all + stores
    }

    console.log(`✅ Agregados atualizados (${uniqueDates.length} dias)`);
  } catch (e) {
    console.error("❌ updateAllAggregatesForDevice:", e?.message || String(e));
  }
}

// ===============================
// 2) SUMMARY (SEM TRAVAR)
// ===============================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    const today = isoDay();
    const sDate = String(start_date || today);
    const eDate = String(end_date || sDate);
    const sid = String(store_id || "all");

    const key = cacheKey(sDate, eDate, sid);
    if (SUMMARY_CACHE.has(key)) {
      return res.status(200).json(SUMMARY_CACHE.get(key));
    }

    // ✅ Regra IMPORTANTÍSSIMA:
    // Só permite ingest dentro do summary quando o período é HOJE (mesmo dia).
    const allowIngest =
      (process.env.SUMMARY_ENABLE_INGEST || "1") === "1" &&
      sDate === today &&
      eDate === today;

    return await calculateRealTimeSummary(res, sDate, eDate, sid, { allowIngest });
  } catch (error) {
    console.error("❌ Summary error:", error);
    return res.status(200).json(createEmptySummary());
  }
}

// ===============================
// 3) HOURLY (por visitors/local_time)
// ===============================
async function getHourlyAggregatesWithRealTime(start_date, end_date, store_id) {
  try {
    const hourExpr = `EXTRACT(HOUR FROM local_time::time)`;

    let query = `
      SELECT 
        ${hourExpr} AS hour,
        COUNT(*) AS total,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female
      FROM visitors
      WHERE day >= $1 AND day <= $2 AND local_time IS NOT NULL
    `;
    const params = [start_date, end_date];

    if (store_id && store_id !== "all") {
      query += ` AND store_id = $3`;
      params.push(store_id);
    }

    query += ` GROUP BY ${hourExpr} ORDER BY ${hourExpr}`;

    const result = await pool.query(query, params);

    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }

    for (const row of result.rows) {
      const hour = Number(row.hour);
      if (hour >= 0 && hour < 24) {
        byHour[hour] = Number(row.total || 0);
        byGenderHour.male[hour] = Number(row.male || 0);
        byGenderHour.female[hour] = Number(row.female || 0);
      }
    }

    // ✅ Shift só de exibição
    if (CHART_HOUR_SHIFT) {
      return shiftHourlyBuckets(byHour, byGenderHour, CHART_HOUR_SHIFT);
    }

    return { byHour, byGenderHour };
  } catch (e) {
    console.error("❌ getHourlyAggregatesWithRealTime:", e?.message || String(e));
    return createEmptyHourlyData();
  }
}

async function getHourlyAggregatesFromAggregates(start_date, end_date, store_id) {
  try {
    const sid = store_id || "all";

    let { rows } = await pool.query(
      `
      SELECT hour,
             COALESCE(SUM(total),0) AS total,
             COALESCE(SUM(male),0) AS male,
             COALESCE(SUM(female),0) AS female
      FROM dashboard_hourly
      WHERE day >= $1 AND day <= $2 AND store_id = $3
      GROUP BY hour
      ORDER BY hour
      `,
      [start_date, end_date, sid]
    );

    // fallback: recalcula de visitors se não tiver agregado
    if (!rows || rows.length === 0) {
      const adj = `COALESCE(EXTRACT(HOUR FROM local_time::time), EXTRACT(HOUR FROM (timestamp + INTERVAL '${TIMEZONE_OFFSET_HOURS} hour')))`;
      let vq = `
        SELECT ${adj} AS hour,
               COUNT(*) AS total,
               SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
               SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female
        FROM visitors
        WHERE day >= $1 AND day <= $2
      `;
      const params = [start_date, end_date];
      if (sid !== "all") {
        vq += ` AND store_id = $3`;
        params.push(sid);
      }
      vq += ` GROUP BY ${adj} ORDER BY 1`;
      const r2 = await pool.query(vq, params);
      rows = r2.rows;
    }

    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }

    for (const r of rows) {
      const h = Number(r.hour);
      if (h >= 0 && h < 24) {
        byHour[h] = Number(r.total || 0);
        byGenderHour.male[h] = Number(r.male || 0);
        byGenderHour.female[h] = Number(r.female || 0);
      }
    }

    if (CHART_HOUR_SHIFT) {
      return shiftHourlyBuckets(byHour, byGenderHour, CHART_HOUR_SHIFT);
    }

    return { byHour, byGenderHour };
  } catch (e) {
    console.error("❌ getHourlyAggregatesFromAggregates:", e?.message || String(e));
    return createEmptyHourlyData();
  }
}

// ===============================
// 4) CALC SUMMARY (com allowIngest)
// ===============================
async function calculateRealTimeSummary(res, start_date, end_date, store_id, opts = {}) {
  try {
    const today = isoDay();
    const sDate = start_date || today;
    const eDate = end_date || sDate;
    const sid = store_id || "all";
    const allowIngest = !!opts.allowIngest;

    console.log(`🧮 Summary ${sDate} - ${eDate} store=${sid} allowIngest=${allowIngest}`);

    let query = `
      SELECT 
        COUNT(*) AS total_visitors,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female,
        SUM(age) AS avg_age_sum,
        SUM(CASE WHEN age > 0 THEN 1 ELSE 0 END) AS avg_age_count,
        SUM(CASE WHEN age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
        SUM(CASE WHEN age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
        SUM(CASE WHEN age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
        SUM(CASE WHEN age BETWEEN 46 AND 60 THEN 1 ELSE 0 END) AS age_46_60,
        SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS age_60_plus,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 0 THEN 1 ELSE 0 END) AS sunday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 1 THEN 1 ELSE 0 END) AS monday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 2 THEN 1 ELSE 0 END) AS tuesday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 3 THEN 1 ELSE 0 END) AS wednesday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 4 THEN 1 ELSE 0 END) AS thursday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 5 THEN 1 ELSE 0 END) AS friday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 6 THEN 1 ELSE 0 END) AS saturday
      FROM visitors
      WHERE day >= $1 AND day <= $2
    `;
    const params = [sDate, eDate];
    if (sid !== "all") {
      query += ` AND store_id = $3`;
      params.push(sid);
    }

    let result = await pool.query(query, params);
    let row = result.rows[0] || {};

    let totalRealTime = Number(row.total_visitors || 0);
    const avgAgeCount = Number(row.avg_age_count || 0);
    let averageAge = avgAgeCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgAgeCount) : 0;

    // ✅ Ingest apenas quando for HOJE (via getSummary)
    if (allowIngest && DISPLAYFORCE_TOKEN) {
      try {
        const startISO = tzISO(sDate, "00:00:00");
        const endISO = tzISO(eDate, "23:59:59");

        const firstBody = { start: startISO, end: endISO, limit: 500, offset: 0, tracks: true };
        if (sid !== "all") firstBody.devices = [parseInt(sid, 10)];

        const firstResp = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
          method: "POST",
          headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
          body: JSON.stringify(firstBody),
        });

        if (firstResp.ok) {
          const firstData = await firstResp.json();
          const limit = Number(firstData.pagination?.limit ?? 500);
          const apiTotal = Number(
            firstData.pagination?.total ??
              (Array.isArray(firstData.payload) ? firstData.payload.length : 0)
          );

          const missing = Math.max(0, apiTotal - totalRealTime);
          console.log(`📡 API total=${apiTotal} DB total=${totalRealTime} faltando=${missing}`);

          if (missing > 0) {
            const startOffset = Math.floor(totalRealTime / limit) * limit;
            const endOffset = Math.floor((apiTotal - 1) / limit) * limit;

            const maxPages = Math.max(
              1,
              Math.min(parseInt(String(process.env.SUMMARY_INGEST_MAX_PAGES || "8"), 10) || 8, 64)
            );

            let processed = 0;
            for (let off = startOffset; off <= endOffset && processed < maxPages; off += limit) {
              const r = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
                method: "POST",
                headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
                body: JSON.stringify({
                  start: startISO,
                  end: endISO,
                  limit,
                  offset: off,
                  tracks: true,
                  ...(sid !== "all" ? { devices: [parseInt(sid, 10)] } : {}),
                }),
              });

              if (!r.ok) break;
              const j = await r.json();
              const arr = j.payload || j || [];
              await saveVisitorsToDatabase(arr, sDate, "one");
              processed++;
              await sleep(120);
            }

            // Recalcula após ingest
            result = await pool.query(query, params);
            row = result.rows[0] || row;

            totalRealTime = Number(row.total_visitors || 0);
            const c2 = Number(row.avg_age_count || 0);
            averageAge = c2 > 0 ? Math.round(Number(row.avg_age_sum || 0) / c2) : 0;

            // Atualiza agregados do dia (pra dashboard_hourly/daily)
            await upsertAggregatesForDate(sDate);
          }
        }
      } catch (e) {
        console.warn("⚠️ allowIngest falhou (ignorado):", e?.message || String(e));
      }
    }

    // Se não tem nada no período, tenta fallback do último dia com dados
    let useStart = sDate;
    let useEnd = eDate;
    let source = "db_realtime";

    if (totalRealTime === 0) {
      try {
        let lastQ = `SELECT MAX(day) AS last_day FROM visitors`;
        const p = [];
        if (sid !== "all") {
          lastQ += ` WHERE store_id = $1`;
          p.push(sid);
        }
        const lr = await q(lastQ, p);
        const lastDay = String(lr.rows?.[0]?.last_day || "");
        if (lastDay) {
          useStart = lastDay;
          useEnd = lastDay;
          source = "fallback_last_available";

          const s2 = await calculateDailyStatsForDate(lastDay, sid);
          totalRealTime = Number(s2.total_visitors || 0);
          averageAge = s2.avg_age_count > 0 ? Math.round(s2.avg_age_sum / s2.avg_age_count) : 0;

          row = {
            total_visitors: s2.total_visitors,
            male: s2.male,
            female: s2.female,
            avg_age_sum: s2.avg_age_sum,
            avg_age_count: s2.avg_age_count,
            age_18_25: s2.age_18_25,
            age_26_35: s2.age_26_35,
            age_36_45: s2.age_36_45,
            age_46_60: s2.age_46_60,
            age_60_plus: s2.age_60_plus,
            sunday: s2.sunday,
            monday: s2.monday,
            tuesday: s2.tuesday,
            wednesday: s2.wednesday,
            thursday: s2.thursday,
            friday: s2.friday,
            saturday: s2.saturday,
          };
        }
      } catch {}
    }

    // ✅ Para histórico: preferir agregados (mais rápido)
    const hourlyData =
      allowIngest
        ? await getHourlyAggregatesWithRealTime(useStart, useEnd, sid)
        : await getHourlyAggregatesFromAggregates(useStart, useEnd, sid);

    const ageGenderData = await getAgeGenderDistribution(useStart, useEnd, sid);

    const response = {
      success: true,
      totalVisitors: totalRealTime,
      totalMale: Number(row.male || 0),
      totalFemale: Number(row.female || 0),
      averageAge,
      visitsByDay: {
        Sunday: Number(row.sunday || 0),
        Monday: Number(row.monday || 0),
        Tuesday: Number(row.tuesday || 0),
        Wednesday: Number(row.wednesday || 0),
        Thursday: Number(row.thursday || 0),
        Friday: Number(row.friday || 0),
        Saturday: Number(row.saturday || 0),
      },
      byAgeGroup: {
        "18-25": Number(row.age_18_25 || 0),
        "26-35": Number(row.age_26_35 || 0),
        "36-45": Number(row.age_36_45 || 0),
        "46-60": Number(row.age_46_60 || 0),
        "60+": Number(row.age_60_plus || 0),
      },
      byAgeGender: ageGenderData,
      byHour: hourlyData.byHour,
      byGenderHour: hourlyData.byGenderHour,
      source,
      period: `${useStart} - ${useEnd}`,
    };

    SUMMARY_CACHE.set(cacheKey(useStart, useEnd, sid), response);
    return res.status(200).json(response);
  } catch (e) {
    console.error("❌ calculateRealTimeSummary:", e?.message || String(e));

    try {
      const today = isoDay();
      const sDate = start_date || today;
      const eDate = end_date || sDate;
      const key = cacheKey(sDate, eDate, store_id || "all");
      if (SUMMARY_CACHE.has(key)) {
        return res.status(200).json({ ...SUMMARY_CACHE.get(key), source: "cache_fallback" });
      }
    } catch {}

    return res.status(200).json(createEmptySummary());
  }
}

// ===============================
// 5) DAILY STATS + UPSERT AGGREGATES
// ===============================
async function calculateDailyStatsForDate(date, store_id) {
  let query = `
    SELECT 
      COUNT(*) AS total_visitors,
      SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
      SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female,
      SUM(age) AS avg_age_sum,
      SUM(CASE WHEN age > 0 THEN 1 ELSE 0 END) AS avg_age_count,
      SUM(CASE WHEN age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
      SUM(CASE WHEN age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
      SUM(CASE WHEN age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
      SUM(CASE WHEN age BETWEEN 46 AND 60 THEN 1 ELSE 0 END) AS age_46_60,
      SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS age_60_plus,
      SUM(CASE WHEN EXTRACT(DOW FROM day) = 0 THEN 1 ELSE 0 END) AS sunday,
      SUM(CASE WHEN EXTRACT(DOW FROM day) = 1 THEN 1 ELSE 0 END) AS monday,
      SUM(CASE WHEN EXTRACT(DOW FROM day) = 2 THEN 1 ELSE 0 END) AS tuesday,
      SUM(CASE WHEN EXTRACT(DOW FROM day) = 3 THEN 1 ELSE 0 END) AS wednesday,
      SUM(CASE WHEN EXTRACT(DOW FROM day) = 4 THEN 1 ELSE 0 END) AS thursday,
      SUM(CASE WHEN EXTRACT(DOW FROM day) = 5 THEN 1 ELSE 0 END) AS friday,
      SUM(CASE WHEN EXTRACT(DOW FROM day) = 6 THEN 1 ELSE 0 END) AS saturday
    FROM visitors
    WHERE day = $1
  `;

  const params = [date];
  if (store_id && store_id !== "all") {
    query += ` AND store_id = $2`;
    params.push(store_id);
  }

  const result = await pool.query(query, params);
  const row = result.rows[0] || {};

  return {
    total_visitors: Number(row.total_visitors || 0),
    male: Number(row.male || 0),
    female: Number(row.female || 0),
    avg_age_sum: Number(row.avg_age_sum || 0),
    avg_age_count: Number(row.avg_age_count || 0),
    age_18_25: Number(row.age_18_25 || 0),
    age_26_35: Number(row.age_26_35 || 0),
    age_36_45: Number(row.age_36_45 || 0),
    age_46_60: Number(row.age_46_60 || 0),
    age_60_plus: Number(row.age_60_plus || 0),
    sunday: Number(row.sunday || 0),
    monday: Number(row.monday || 0),
    tuesday: Number(row.tuesday || 0),
    wednesday: Number(row.wednesday || 0),
    thursday: Number(row.thursday || 0),
    friday: Number(row.friday || 0),
    saturday: Number(row.saturday || 0),
  };
}

async function upsertAggregatesForDate(date) {
  try {
    const adj = `COALESCE(EXTRACT(HOUR FROM local_time::time), EXTRACT(HOUR FROM (timestamp + INTERVAL '${TIMEZONE_OFFSET_HOURS} hour')))`;

    // ALL
    const sAll = await pool.query(
      `
      SELECT 
        COUNT(*) AS total_visitors,
        SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
        SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female,
        SUM(age) AS avg_age_sum,
        SUM(CASE WHEN age>0 THEN 1 ELSE 0 END) AS avg_age_count,
        SUM(CASE WHEN age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
        SUM(CASE WHEN age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
        SUM(CASE WHEN age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
        SUM(CASE WHEN age BETWEEN 46 AND 60 THEN 1 ELSE 0 END) AS age_46_60,
        SUM(CASE WHEN age>60 THEN 1 ELSE 0 END) AS age_60_plus,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 1 THEN 1 ELSE 0 END) AS monday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 2 THEN 1 ELSE 0 END) AS tuesday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 3 THEN 1 ELSE 0 END) AS wednesday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 4 THEN 1 ELSE 0 END) AS thursday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 5 THEN 1 ELSE 0 END) AS friday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 6 THEN 1 ELSE 0 END) AS saturday,
        SUM(CASE WHEN EXTRACT(DOW FROM day) = 0 THEN 1 ELSE 0 END) AS sunday
      FROM visitors WHERE day=$1
      `,
      [date]
    );
    const rAll = sAll.rows[0] || {};

    await pool.query(
      `
      INSERT INTO dashboard_daily (
        day, store_id, total_visitors, male, female, avg_age_sum, avg_age_count,
        age_18_25, age_26_35, age_36_45, age_46_60, age_60_plus,
        monday, tuesday, wednesday, thursday, friday, saturday, sunday
      ) VALUES ($1,'all',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
      ON CONFLICT (day, store_id) DO UPDATE SET
        total_visitors=EXCLUDED.total_visitors, male=EXCLUDED.male, female=EXCLUDED.female,
        avg_age_sum=EXCLUDED.avg_age_sum, avg_age_count=EXCLUDED.avg_age_count,
        age_18_25=EXCLUDED.age_18_25, age_26_35=EXCLUDED.age_26_35, age_36_45=EXCLUDED.age_36_45,
        age_46_60=EXCLUDED.age_46_60, age_60_plus=EXCLUDED.age_60_plus,
        monday=EXCLUDED.monday, tuesday=EXCLUDED.tuesday, wednesday=EXCLUDED.wednesday,
        thursday=EXCLUDED.thursday, friday=EXCLUDED.friday, saturday=EXCLUDED.saturday, sunday=EXCLUDED.sunday
      `,
      [
        date,
        rAll.total_visitors || 0,
        rAll.male || 0,
        rAll.female || 0,
        rAll.avg_age_sum || 0,
        rAll.avg_age_count || 0,
        rAll.age_18_25 || 0,
        rAll.age_26_35 || 0,
        rAll.age_36_45 || 0,
        rAll.age_46_60 || 0,
        rAll.age_60_plus || 0,
        rAll.monday || 0,
        rAll.tuesday || 0,
        rAll.wednesday || 0,
        rAll.thursday || 0,
        rAll.friday || 0,
        rAll.saturday || 0,
        rAll.sunday || 0,
      ]
    );

    const hAll = await pool.query(
      `
      SELECT ${adj} AS hour,
             COUNT(*) AS total,
             SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
             SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female
      FROM visitors WHERE day=$1
      GROUP BY ${adj} ORDER BY 1
      `,
      [date]
    );

    for (const r of hAll.rows) {
      await pool.query(
        `
        INSERT INTO dashboard_hourly (day, store_id, hour, total, male, female)
        VALUES ($1,'all',$2,$3,$4,$5)
        ON CONFLICT (day, store_id, hour) DO UPDATE SET
          total=EXCLUDED.total, male=EXCLUDED.male, female=EXCLUDED.female
        `,
        [date, Number(r.hour), Number(r.total || 0), Number(r.male || 0), Number(r.female || 0)]
      );
    }

    // STORES
    const sStores = await pool.query(`SELECT DISTINCT store_id FROM visitors WHERE day=$1`, [date]);
    for (const st of sStores.rows) {
      const sid = String(st.store_id);

      const s1 = await pool.query(
        `
        SELECT 
          COUNT(*) AS total_visitors,
          SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
          SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female,
          SUM(age) AS avg_age_sum,
          SUM(CASE WHEN age>0 THEN 1 ELSE 0 END) AS avg_age_count,
          SUM(CASE WHEN age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
          SUM(CASE WHEN age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
          SUM(CASE WHEN age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
          SUM(CASE WHEN age BETWEEN 46 AND 60 THEN 1 ELSE 0 END) AS age_46_60,
          SUM(CASE WHEN age>60 THEN 1 ELSE 0 END) AS age_60_plus,
          SUM(CASE WHEN EXTRACT(DOW FROM day) = 1 THEN 1 ELSE 0 END) AS monday,
          SUM(CASE WHEN EXTRACT(DOW FROM day) = 2 THEN 1 ELSE 0 END) AS tuesday,
          SUM(CASE WHEN EXTRACT(DOW FROM day) = 3 THEN 1 ELSE 0 END) AS wednesday,
          SUM(CASE WHEN EXTRACT(DOW FROM day) = 4 THEN 1 ELSE 0 END) AS thursday,
          SUM(CASE WHEN EXTRACT(DOW FROM day) = 5 THEN 1 ELSE 0 END) AS friday,
          SUM(CASE WHEN EXTRACT(DOW FROM day) = 6 THEN 1 ELSE 0 END) AS saturday,
          SUM(CASE WHEN EXTRACT(DOW FROM day) = 0 THEN 1 ELSE 0 END) AS sunday
        FROM visitors WHERE day=$1 AND store_id=$2
        `,
        [date, sid]
      );
      const r1 = s1.rows[0] || {};

      await pool.query(
        `
        INSERT INTO dashboard_daily (
          day, store_id, total_visitors, male, female, avg_age_sum, avg_age_count,
          age_18_25, age_26_35, age_36_45, age_46_60, age_60_plus,
          monday, tuesday, wednesday, thursday, friday, saturday, sunday
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
        ON CONFLICT (day, store_id) DO UPDATE SET
          total_visitors=EXCLUDED.total_visitors, male=EXCLUDED.male, female=EXCLUDED.female,
          avg_age_sum=EXCLUDED.avg_age_sum, avg_age_count=EXCLUDED.avg_age_count,
          age_18_25=EXCLUDED.age_18_25, age_26_35=EXCLUDED.age_26_35, age_36_45=EXCLUDED.age_36_45,
          age_46_60=EXCLUDED.age_46_60, age_60_plus=EXCLUDED.age_60_plus,
          monday=EXCLUDED.monday, tuesday=EXCLUDED.tuesday, wednesday=EXCLUDED.wednesday,
          thursday=EXCLUDED.thursday, friday=EXCLUDED.friday, saturday=EXCLUDED.saturday, sunday=EXCLUDED.sunday
        `,
        [
          date,
          sid,
          r1.total_visitors || 0,
          r1.male || 0,
          r1.female || 0,
          r1.avg_age_sum || 0,
          r1.avg_age_count || 0,
          r1.age_18_25 || 0,
          r1.age_26_35 || 0,
          r1.age_36_45 || 0,
          r1.age_46_60 || 0,
          r1.age_60_plus || 0,
          r1.monday || 0,
          r1.tuesday || 0,
          r1.wednesday || 0,
          r1.thursday || 0,
          r1.friday || 0,
          r1.saturday || 0,
          r1.sunday || 0,
        ]
      );

      const hRows = await pool.query(
        `
        SELECT ${adj} AS hour,
               COUNT(*) AS total,
               SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
               SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female
        FROM visitors WHERE day=$1 AND store_id=$2
        GROUP BY ${adj} ORDER BY 1
        `,
        [date, sid]
      );

      for (const rr of hRows.rows) {
        await pool.query(
          `
          INSERT INTO dashboard_hourly (day, store_id, hour, total, male, female)
          VALUES ($1,$2,$3,$4,$5,$6)
          ON CONFLICT (day, store_id, hour) DO UPDATE SET
            total=EXCLUDED.total, male=EXCLUDED.male, female=EXCLUDED.female
          `,
          [
            date,
            sid,
            Number(rr.hour),
            Number(rr.total || 0),
            Number(rr.male || 0),
            Number(rr.female || 0),
          ]
        );
      }
    }
  } catch (e) {
    console.warn("⚠️ upsertAggregatesForDate falhou:", e?.message || String(e));
  }
}

// ===============================
// 6) SAVE VISITORS (corrigido batch insert)
// ===============================
async function saveVisitorsToDatabase(visitors, forcedDay, mode) {
  if (!visitors || !Array.isArray(visitors) || visitors.length === 0) return 0;

  const tz = TIMEZONE_OFFSET_HOURS;
  const DAYS = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"];
  const records = [];

  for (const visitor of visitors) {
    try {
      const timestamp = String(
        visitor.start ?? visitor.tracks?.[0]?.start ?? visitor.timestamp ?? new Date().toISOString()
      );
      const dateObj = new Date(timestamp);
      if (isNaN(dateObj.getTime())) continue;

      const localDate = new Date(dateObj.getTime() + tz * 3600000);

      const hh = String(localDate.getHours()).padStart(2, "0");
      const mm = String(localDate.getMinutes()).padStart(2, "0");
      const ss = String(localDate.getSeconds()).padStart(2, "0");
      const localTime = `${hh}:${mm}:${ss}`;

      const y = localDate.getFullYear();
      const m = String(localDate.getMonth() + 1).padStart(2, "0");
      const d = String(localDate.getDate()).padStart(2, "0");
      const dateStr = String(forcedDay || `${y}-${m}-${d}`);
      const dayOfWeek = DAYS[localDate.getDay()];

      let deviceId = "";
      let storeName = "";
      const t0 = visitor.tracks && visitor.tracks.length > 0 ? visitor.tracks[0] : null;
      if (t0) {
        deviceId = String(t0.device_id ?? t0.id ?? "");
        storeName = String(t0.device_name ?? t0.name ?? "");
      }
      if (!deviceId && visitor.devices && visitor.devices.length > 0) {
        const dev0 = visitor.devices[0];
        if (typeof dev0 === "object" && dev0) {
          deviceId = String(dev0.id ?? dev0.device_id ?? "");
          storeName = String(dev0.name ?? storeName);
        } else {
          deviceId = String(dev0 || "");
        }
      }
      if (!deviceId) deviceId = "unknown";
      if (!storeName) storeName = `Loja ${deviceId}`;

      let gender = "U";
      const sexNum =
        typeof visitor.sex === "number"
          ? visitor.sex
          : typeof visitor.gender === "number"
          ? visitor.gender
          : null;
      if (sexNum === 1) gender = "M";
      else if (sexNum === 2) gender = "F";
      else {
        const gRaw = String(visitor.gender || "").toUpperCase();
        if (gRaw.startsWith("M")) gender = "M";
        else if (gRaw.startsWith("F")) gender = "F";
      }

      let age = 0;
      if (typeof visitor.age === "number") age = Math.max(0, visitor.age);
      else {
        const attrsA = Array.isArray(visitor.additional_attributes)
          ? visitor.additional_attributes
          : visitor.additional_attributes && typeof visitor.additional_attributes === "object"
          ? [visitor.additional_attributes]
          : [];
        const attrsB = Array.isArray(visitor.additional_atributes)
          ? visitor.additional_atributes
          : visitor.additional_atributes && typeof visitor.additional_atributes === "object"
          ? [visitor.additional_atributes]
          : [];
        const attrsAll = [...attrsA, ...attrsB];
        const lastAttr = attrsAll.length ? attrsAll[attrsAll.length - 1] : null;
        const ageCandidate = lastAttr?.age ?? visitor.face?.age ?? visitor.age_years;
        if (typeof ageCandidate === "number") age = Math.max(0, ageCandidate);
      }

      let smile = false;
      const attrs = visitor.additional_atributes || visitor.additional_attributes || [];
      if (Array.isArray(attrs) ? attrs.length > 0 : typeof attrs === "object") {
        const lastAttr = Array.isArray(attrs) ? attrs[attrs.length - 1] : attrs;
        smile = String(lastAttr?.smile || "").toLowerCase() === "yes";
      }

      const visitorId = String(
        visitor.visitor_id ??
          visitor.session_id ??
          visitor.id ??
          visitor.tracks?.[0]?.id ??
          `${deviceId}|${timestamp}`
      );

      records.push([
        visitorId, // 0
        dateStr, // 1
        deviceId, // 2
        storeName, // 3
        timestamp, // 4
        gender, // 5
        age, // 6
        dayOfWeek, // 7
        smile, // 8
        localTime, // 9
      ]);
    } catch {}
  }

  if (records.length === 0) return 0;

  const BATCH_SIZE = 200;
  let savedCount = 0;
  const single = process.env.INSERT_ONE_BY_ONE === "1" || String(mode || "").toLowerCase() === "one";

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const chunk = records.slice(i, i + BATCH_SIZE);

    if (single) {
      for (const r of chunk) {
        const sql1 = `
          INSERT INTO visitors (
            visitor_id, day, store_id, store_name, timestamp, gender, age, day_of_week, smile, hour, local_time
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,EXTRACT(HOUR FROM $10::time),$10::time)
          ON CONFLICT (visitor_id, timestamp) DO UPDATE SET
            day=EXCLUDED.day,
            store_id=EXCLUDED.store_id,
            store_name=EXCLUDED.store_name,
            gender=EXCLUDED.gender,
            age=EXCLUDED.age,
            day_of_week=EXCLUDED.day_of_week,
            smile=EXCLUDED.smile,
            hour=EXTRACT(HOUR FROM EXCLUDED.local_time::time),
            local_time=EXCLUDED.local_time
        `;
        try {
          await q(sql1, r);
          savedCount += 1;
        } catch {}
      }
    } else {
      // ✅ FIX CRÍTICO: placeholders PRECISAM ser $1, $2...
      const params = [];
      const values = chunk
        .map((r, idx) => {
          const base = idx * 10;
          params.push(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]);
          return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, EXTRACT(HOUR FROM $${base + 10}::time), $${base + 10}::time)`;
        })
        .join(", ");

      const sql = `
        INSERT INTO visitors (
          visitor_id, day, store_id, store_name, timestamp, gender, age, day_of_week, smile, hour, local_time
        )
        VALUES ${values}
        ON CONFLICT (visitor_id, timestamp) DO UPDATE SET
          day=EXCLUDED.day,
          store_id=EXCLUDED.store_id,
          store_name=EXCLUDED.store_name,
          gender=EXCLUDED.gender,
          age=EXCLUDED.age,
          day_of_week=EXCLUDED.day_of_week,
          smile=EXCLUDED.smile,
          hour=EXTRACT(HOUR FROM EXCLUDED.local_time::time),
          local_time=EXCLUDED.local_time
      `;

      try {
        await q(sql, params);
        savedCount += chunk.length;
      } catch {}
    }
  }

  return savedCount;
}

// ===============================
// 7) AUX: AGE/GENDER
// ===============================
async function getAgeGenderDistribution(start_date, end_date, store_id) {
  try {
    let query = `
      SELECT gender, age
      FROM visitors
      WHERE age > 0 AND day >= $1 AND day <= $2
    `;
    const params = [start_date, end_date];
    if (store_id && store_id !== "all") {
      query += ` AND store_id = $3`;
      params.push(store_id);
    }

    const result = await pool.query(query, params);

    const byAgeGender = {
      "<20": { male: 0, female: 0 },
      "20-29": { male: 0, female: 0 },
      "30-45": { male: 0, female: 0 },
      ">45": { male: 0, female: 0 },
    };

    for (const row of result.rows) {
      const gender = row.gender === "M" ? "male" : "female";
      const age = Number(row.age || 0);
      if (age < 20) byAgeGender["<20"][gender]++;
      else if (age <= 29) byAgeGender["20-29"][gender]++;
      else if (age <= 45) byAgeGender["30-45"][gender]++;
      else byAgeGender[">45"][gender]++;
    }

    return byAgeGender;
  } catch {
    return createEmptyAgeGender();
  }
}

// ===============================
// 8) VISITORS (lista)
// ===============================
async function getVisitors(req, res, start_date, end_date, store_id) {
  try {
    // query no DB
    let query = `
      SELECT visitor_id, day, store_id, store_name, timestamp, gender, age, day_of_week, smile, hour, local_time
      FROM visitors
      WHERE 1=1
    `;
    const params = [];
    let p = 1;

    if (start_date) {
      query += ` AND day >= $${p++}`;
      params.push(start_date);
    }
    if (end_date) {
      query += ` AND day <= $${p++}`;
      params.push(end_date);
    }
    if (store_id && store_id !== "all") {
      query += ` AND store_id = $${p++}`;
      params.push(store_id);
    }

    // ⚠️ se a sua tela usa paginação, NÃO use LIMIT fixo assim no front.
    // Aqui deixei 1000 como “proteção”. Você pode trocar por paginação via query param.
    query += ` ORDER BY timestamp DESC LIMIT 1000`;

    let result = await pool.query(query, params);

    return res.status(200).json({
      success: true,
      data: result.rows.map((row) => ({
        id: row.visitor_id,
        date: row.day,
        store_id: row.store_id,
        store_name: row.store_name,
        timestamp: row.timestamp,
        gender: row.gender === "M" ? "Masculino" : row.gender === "F" ? "Feminino" : "Desconhecido",
        age: row.age,
        day_of_week: row.day_of_week,
        smile: row.smile,
        hour: row.hour,
        local_time: row.local_time, // horário já “local” (TIMEZONE_OFFSET_HOURS)
      })),
      count: result.rows.length,
      source: "database",
    });
  } catch (e) {
    console.error("❌ Visitors error:", e?.message || String(e));
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

// ===============================
// 9) STORES / DEVICES
// ===============================
async function getStores(req, res) {
  try {
    const devices = await fetchDisplayForceDevices();

    const storesWithCount = await Promise.all(
      devices.map(async (device) => {
        const countResult = await pool.query("SELECT COUNT(*) FROM visitors WHERE store_id = $1", [
          device.id,
        ]);
        return { ...device, visitor_count: parseInt(countResult.rows[0].count || 0, 10) };
      })
    );

    return res.status(200).json({ success: true, stores: storesWithCount, count: storesWithCount.length });
  } catch (e) {
    return res.status(200).json({ success: true, stores: [], isFallback: true });
  }
}

async function fetchDisplayForceDevices() {
  try {
    if (!DISPLAYFORCE_TOKEN) return [];

    let response = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/device/list`, {
      method: "POST",
      headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });

    if (!response.ok && response.status === 405) {
      response = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/device/list`, {
        method: "GET",
        headers: { "X-API-Token": DISPLAYFORCE_TOKEN },
      });
    }

    if (!response.ok) throw new Error(`DisplayForce API: ${response.status}`);

    const data = await response.json();
    const devices = data.devices || data.data || [];

    return devices.map((device) => ({
      id: String(device.id || device.device_id || ""),
      name: device.name || `Dispositivo ${device.id || device.device_id}`,
      location: device.location || "Local desconhecido",
      status: device.status || "active",
    }));
  } catch (e) {
    console.error("❌ fetchDisplayForceDevices:", e?.message || String(e));
    return [];
  }
}

async function getDevices(req, res) {
  const devices = await fetchDisplayForceDevices();
  return res.status(200).json({ success: true, devices, count: devices.length });
}

// ===============================
// 10) REFRESH / FETCH VISITORS (DisplayForce)
// ===============================
async function refreshRange(req, res, start_date, end_date, store_id) {
  try {
    const s = start_date || isoDay();
    const e = end_date || s;

    const visitors = await fetchVisitorsFromDisplayForce(s, e, store_id && store_id !== "all" ? store_id : null);
    const saved = await saveVisitorsToDatabase(visitors);

    // Atualiza agregados do período
    const start = new Date(`${s}T00:00:00Z`);
    const end = new Date(`${e}T00:00:00Z`);
    for (let d = new Date(start); d <= end; d = new Date(d.getTime() + 86400000)) {
      await upsertAggregatesForDate(d.toISOString().slice(0, 10));
    }

    // limpa cache do summary do período (best-effort)
    SUMMARY_CACHE.clear();

    return res.status(200).json({
      success: true,
      message: "Refresh concluído",
      period: `${s} - ${e}`,
      visitors_found: visitors.length,
      visitors_saved: saved,
      store_id: store_id || "all",
    });
  } catch (e) {
    console.error("❌ refreshRange:", e?.message || String(e));
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

async function fetchVisitorsFromDisplayForce(start_date, end_date, device_id = null) {
  if (!DISPLAYFORCE_TOKEN) throw new Error("DISPLAYFORCE_API_TOKEN não configurado");

  const startISO = tzISO(start_date, "00:00:00");
  const endISO = tzISO(end_date, "23:59:59");

  const LIMIT = 500;

  console.log(`🔍 DisplayForce list: ${startISO} -> ${endISO} device=${device_id || "all"}`);

  // 1) primeira página
  const firstBody = { start: startISO, end: endISO, limit: LIMIT, offset: 0, tracks: true };
  if (device_id) firstBody.devices = [parseInt(device_id, 10)];

  const firstResp = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
    method: "POST",
    headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(firstBody),
  });

  if (!firstResp.ok) throw new Error(`API Error ${firstResp.status}: ${await firstResp.text().catch(() => "")}`);

  const firstData = await firstResp.json();
  const firstArr = firstData.payload || firstData || [];

  const pageLimit = Number(firstData.pagination?.limit ?? LIMIT);
  const totalFromAPI = Number(firstData.pagination?.total ?? firstArr.length);

  const all = [...firstArr];

  // 2) offsets restantes (paralelo com limite)
  const offsets = [];
  for (let off = pageLimit; off < totalFromAPI; off += pageLimit) offsets.push(off);

  const CONCURRENCY = Math.max(1, Math.min(parseInt(process.env.FETCH_CONCURRENCY || "6", 10), 10));
  let idx = 0;

  while (idx < offsets.length) {
    const batch = offsets.slice(idx, idx + CONCURRENCY);
    const calls = batch.map(async (off) => {
      const bodyPayload = { start: startISO, end: endISO, limit: pageLimit, offset: off, tracks: true };
      if (device_id) bodyPayload.devices = [parseInt(device_id, 10)];

      const r = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
        method: "POST",
        headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
        body: JSON.stringify(bodyPayload),
      });

      if (!r.ok) return [];
      const j = await r.json();
      return j.payload || j || [];
    });

    const got = await Promise.all(calls);
    for (const arr of got) all.push(...arr);

    idx += CONCURRENCY;
  }

  console.log(`✅ DisplayForce total obtido: ${all.length} (apiTotal=${totalFromAPI})`);
  return all;
}

async function refreshAll(req, res, start_date, end_date) {
  try {
    const s = start_date || isoDay();
    const e = end_date || s;

    const visitors = await fetchVisitorsFromDisplayForce(s, e, null);
    const saved = await saveVisitorsToDatabase(visitors, s, String(req.query.mode || ""));

    // agregados
    const start = new Date(`${s}T00:00:00Z`);
    const end = new Date(`${e}T00:00:00Z`);
    for (let d = new Date(start); d <= end; d = new Date(d.getTime() + 86400000)) {
      await upsertAggregatesForDate(d.toISOString().slice(0, 10));
    }

    SUMMARY_CACHE.clear();

    return res.status(200).json({
      success: true,
      period: `${s} - ${e}`,
      visitors_found: visitors.length,
      visitors_saved: saved,
    });
  } catch (e) {
    console.error("❌ refreshAll:", e?.message || String(e));
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

// ===============================
// 11) AUTO / PLAN / INGEST / FORCE TODAY
// ===============================
async function autoRefresh(req, res) {
  try {
    const s = String(req.query.start_date || "");
    const e = String(req.query.end_date || "");
    const start = s || isoDay(new Date(Date.now() - 86400000));
    const end = e || start;

    const proto = String(req.headers["x-forwarded-proto"] || "https");
    const host = String(req.headers["host"] || "");
    const base = host ? `${proto}://${host}` : "";

    const days = [];
    for (let d = new Date(`${start}T00:00:00Z`); d <= new Date(`${end}T00:00:00Z`); d = new Date(d.getTime() + 86400000)) {
      days.push(d.toISOString().slice(0, 10));
    }

    const calls = [];
    for (const day of days) {
      if (base) {
        calls.push(fetch(`${base}/api/assai/dashboard?endpoint=force_sync_today&mode=one&concurrency=1&max_pages=16&start_date=${day}`).catch(() => {}));
        calls.push(fetch(`${base}/api/assai/dashboard?endpoint=refresh_recent&start_date=${day}&store_id=all&count=1&mode=one`).catch(() => {}));
      }
    }

    return res.status(202).json({ success: true, triggered: calls.length, start, end });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

async function planIngestDay(req, res, start_date, end_date, store_id) {
  try {
    if (!DISPLAYFORCE_TOKEN) throw new Error("DISPLAYFORCE_API_TOKEN não configurado");

    const day = start_date || isoDay();
    const startISO = tzISO(day, "00:00:00");
    const endISO = tzISO(day, "23:59:59");

    const body = { start: startISO, end: endISO, limit: 500, offset: 0, tracks: true };
    if (store_id && store_id !== "all") body.devices = [parseInt(store_id, 10)];

    const r = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: "POST",
      headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!r.ok) return res.status(r.status).json({ error: await r.text().catch(() => "") });

    const j = await r.json();
    const limit = Number(j.pagination?.limit ?? 500);
    const total = Number(j.pagination?.total ?? (Array.isArray(j.payload) ? j.payload.length : 0));

    const offsets = [];
    for (let off = 0; off < total; off += limit) offsets.push(off);

    return res.status(200).json({ day, limit, total, offsets });
  } catch (e) {
    return res.status(500).json({ error: e?.message || String(e) });
  }
}

async function ingestDay(req, res, start_date, end_date, store_id) {
  try {
    if (!DISPLAYFORCE_TOKEN) throw new Error("DISPLAYFORCE_API_TOKEN não configurado");

    const day = start_date || isoDay();
    const offset = Number(req.query.offset || 0);
    const limit = Number(req.query.limit || 500);

    const startISO = tzISO(day, "00:00:00");
    const endISO = tzISO(day, "23:59:59");

    const body = { start: startISO, end: endISO, limit, offset, tracks: true };
    if (store_id && store_id !== "all") body.devices = [parseInt(store_id, 10)];

    const r = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: "POST",
      headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!r.ok) return res.status(r.status).json({ error: await r.text().catch(() => "") });

    const j = await r.json();
    const arr = j.payload || j || [];

    const saved = await saveVisitorsToDatabase(arr, day, String(req.query.mode || ""));
    await upsertAggregatesForDate(day);
    SUMMARY_CACHE.clear();

    return res.status(200).json({ day, offset, limit, saved, count: arr.length });
  } catch (e) {
    return res.status(500).json({ error: e?.message || String(e) });
  }
}

async function forceSyncToday(req, res) {
  try {
    if (!DISPLAYFORCE_TOKEN) throw new Error("DISPLAYFORCE_API_TOKEN não configurado");

    const day = String(req.query.start_date || isoDay());

    const startISO = tzISO(day, "00:00:00");
    const endISO = tzISO(day, "23:59:59");

    const firstBody = { start: startISO, end: endISO, limit: 500, offset: 0, tracks: true };

    const firstResp = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: "POST",
      headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify(firstBody),
    });

    if (!firstResp.ok) return res.status(firstResp.status).json({ error: await firstResp.text().catch(() => "") });

    const firstData = await firstResp.json();
    const limit = Number(firstData.pagination?.limit ?? 500);
    const apiTotal = Number(firstData.pagination?.total ?? 0);

    const { rows } = await q(`SELECT COUNT(*)::int AS c FROM visitors WHERE day=$1`, [day]);
    const dbTotal = Number(rows[0]?.c || 0);

    if (dbTotal >= apiTotal) {
      await upsertAggregatesForDate(day);
      return res.status(200).json({ success: true, day, apiTotal, dbTotal, synced: true });
    }

    const startOffset = Math.floor(dbTotal / limit) * limit;
    const endOffset = Math.floor((apiTotal - 1) / limit) * limit;

    const offsets = [];
    for (let off = startOffset; off <= endOffset; off += limit) offsets.push(off);

    const conc = Math.max(1, Math.min(parseInt(String(req.query.concurrency || "1"), 10) || 1, 4));
    const maxPages = Math.max(1, Math.min(parseInt(String(req.query.max_pages || "16"), 10) || 16, 128));

    const slice = offsets.slice(0, maxPages);

    let processed = 0;
    while (processed < slice.length) {
      const batch = slice.slice(processed, processed + conc);

      const calls = batch.map(async (off) => {
        const r = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
          method: "POST",
          headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
          body: JSON.stringify({ start: startISO, end: endISO, limit, offset: off, tracks: true }),
        });

        if (!r.ok) return 0;
        const j = await r.json();
        const arr = j.payload || j || [];
        return await saveVisitorsToDatabase(arr, day, String(req.query.mode || ""));
      });

      await Promise.all(calls);
      processed += batch.length;
      await sleep(150);
    }

    await upsertAggregatesForDate(day);
    SUMMARY_CACHE.clear();

    const vr = await pool.query(`SELECT COUNT(*)::int AS c FROM visitors WHERE day=$1`, [day]);
    return res.status(200).json({
      success: true,
      day,
      apiTotal,
      dbTotal_before: dbTotal,
      dbTotal_after: Number(vr.rows[0]?.c || 0),
      processed_pages: slice.length,
    });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

// ===============================
// 12) WIPE / VERIFY / REBUILD / RECENT
// ===============================
async function wipeRange(req, res, start_date, end_date) {
  try {
    const s = start_date || isoDay();
    const e = end_date || s;

    const delH = await pool.query(`DELETE FROM dashboard_hourly WHERE day BETWEEN $1 AND $2`, [s, e]);
    const delD = await pool.query(`DELETE FROM dashboard_daily  WHERE day BETWEEN $1 AND $2`, [s, e]);
    const delV = await pool.query(`DELETE FROM visitors        WHERE day BETWEEN $1 AND $2`, [s, e]);

    SUMMARY_CACHE.clear();

    return res.status(200).json({
      success: true,
      period: `${s} - ${e}`,
      deleted: { hourly: delH.rowCount, daily: delD.rowCount, visitors: delV.rowCount },
    });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

async function verifyDay(req, res, start_date, store_id) {
  try {
    if (!DISPLAYFORCE_TOKEN) throw new Error("DISPLAYFORCE_API_TOKEN não configurado");

    const day = start_date || isoDay();

    const startISO = tzISO(day, "00:00:00");
    const endISO = tzISO(day, "23:59:59");

    const body = { start: startISO, end: endISO, limit: 1, offset: 0, tracks: true };
    if (store_id && store_id !== "all") body.devices = [parseInt(store_id, 10)];

    const r = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: "POST",
      headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!r.ok) return res.status(r.status).json({ error: await r.text().catch(() => "") });

    const j = await r.json();
    const apiTotal = Number(j.pagination?.total || 0);

    const db = await q(
      store_id && store_id !== "all"
        ? `SELECT 1 AS found FROM visitors WHERE day=$1 AND store_id=$2 LIMIT 1`
        : `SELECT 1 AS found FROM visitors WHERE day=$1 LIMIT 1`,
      store_id && store_id !== "all" ? [day, store_id] : [day]
    );

    const dbHas = db.rows && db.rows.length > 0;
    return res.status(200).json({ day, apiTotal, dbHas, ok: dbHas && apiTotal > 0 });
  } catch (e) {
    return res.status(200).json({ day: start_date || isoDay(), ok: false, error: e?.message || String(e) });
  }
}

async function rebuildHourlyFromVisitors(req, res, start_date, end_date, store_id) {
  try {
    const s = start_date || isoDay();
    const e = end_date || s;
    const sid = store_id || "all";

    const start = new Date(`${s}T00:00:00Z`);
    const end = new Date(`${e}T00:00:00Z`);

    for (let d = new Date(start); d <= end; d = new Date(d.getTime() + 86400000)) {
      await upsertAggregatesForDate(d.toISOString().slice(0, 10));
    }

    SUMMARY_CACHE.clear();
    return res.status(200).json({ success: true, period: `${s} - ${e}`, store_id: sid });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

async function refreshRecent(req, res, start_date, store_id) {
  try {
    if (!DISPLAYFORCE_TOKEN) throw new Error("DISPLAYFORCE_API_TOKEN não configurado");

    const day = start_date || isoDay();
    const sid = store_id || "all";

    const startISO = tzISO(day, "00:00:00");
    const endISO = tzISO(day, "23:59:59");

    const body = { start: startISO, end: endISO, limit: 500, offset: 0, tracks: true };
    if (sid !== "all") body.devices = [parseInt(sid, 10)];

    const r = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: "POST",
      headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) return res.status(r.status).json({ error: await r.text().catch(() => "") });

    const j = await r.json();
    const limit = Number(j.pagination?.limit ?? 500);
    const total = Number(j.pagination?.total ?? (Array.isArray(j.payload) ? j.payload.length : 0));

    const offsets = [];
    for (let off = 0; off < total; off += limit) offsets.push(off);

    const recentCount = Math.max(1, Number(req.query.count || 24));
    const slice = offsets.slice(0, recentCount);

    const results = await Promise.all(
      slice.map(async (off) => {
        const jr = await fetchWithTimeout(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
          method: "POST",
          headers: { "X-API-Token": DISPLAYFORCE_TOKEN, "Content-Type": "application/json" },
          body: JSON.stringify({
            start: startISO,
            end: endISO,
            limit,
            offset: off,
            tracks: true,
            ...(sid !== "all" ? { devices: [parseInt(sid, 10)] } : {}),
          }),
        });

        if (!jr.ok) return { saved: 0, processed: 0 };
        const jj = await jr.json();
        const arr = jj.payload || jj || [];
        const saved = await saveVisitorsToDatabase(arr, day, String(req.query.mode || ""));
        return { saved, processed: arr.length };
      })
    );

    const saved = results.reduce((a, b) => a + b.saved, 0);
    const processed = results.reduce((a, b) => a + b.processed, 0);

    await upsertAggregatesForDate(day);
    SUMMARY_CACHE.clear();

    return res.status(200).json({ success: true, day, store_id: sid, processed, saved });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

// ===============================
// 13) OPTIMIZE (cria tabelas/índices)
// ===============================
async function ensureIndexes(req, res) {
  try {
    console.log("🔧 Criando tabelas/índices...");

    await pool.query(`
      CREATE TABLE IF NOT EXISTS visitors (
        visitor_id TEXT NOT NULL,
        day DATE,
        store_id TEXT,
        store_name TEXT,
        timestamp TIMESTAMPTZ NOT NULL,
        gender TEXT,
        age INT,
        day_of_week TEXT,
        smile BOOLEAN,
        hour SMALLINT,
        local_time TIME,
        PRIMARY KEY (visitor_id, timestamp)
      );

      CREATE TABLE IF NOT EXISTS dashboard_daily (
        day DATE NOT NULL,
        store_id TEXT NOT NULL,
        total_visitors INT NOT NULL DEFAULT 0,
        male INT NOT NULL DEFAULT 0,
        female INT NOT NULL DEFAULT 0,
        avg_age_sum INT NOT NULL DEFAULT 0,
        avg_age_count INT NOT NULL DEFAULT 0,
        age_18_25 INT NOT NULL DEFAULT 0,
        age_26_35 INT NOT NULL DEFAULT 0,
        age_36_45 INT NOT NULL DEFAULT 0,
        age_46_60 INT NOT NULL DEFAULT 0,
        age_60_plus INT NOT NULL DEFAULT 0,
        monday INT NOT NULL DEFAULT 0,
        tuesday INT NOT NULL DEFAULT 0,
        wednesday INT NOT NULL DEFAULT 0,
        thursday INT NOT NULL DEFAULT 0,
        friday INT NOT NULL DEFAULT 0,
        saturday INT NOT NULL DEFAULT 0,
        sunday INT NOT NULL DEFAULT 0,
        PRIMARY KEY (day, store_id)
      );

      CREATE TABLE IF NOT EXISTS dashboard_hourly (
        day DATE NOT NULL,
        store_id TEXT NOT NULL,
        hour SMALLINT NOT NULL,
        total INT NOT NULL DEFAULT 0,
        male INT NOT NULL DEFAULT 0,
        female INT NOT NULL DEFAULT 0,
        PRIMARY KEY (day, store_id, hour)
      );

      CREATE INDEX IF NOT EXISTS idx_visitors_day ON visitors(day);
      CREATE INDEX IF NOT EXISTS idx_visitors_store_id ON visitors(store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_day_store ON visitors(day, store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_timestamp ON visitors(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_visitors_gender ON visitors(gender);
      CREATE INDEX IF NOT EXISTS idx_visitors_age ON visitors(age);
      CREATE INDEX IF NOT EXISTS idx_visitors_hour ON visitors(hour);
      CREATE INDEX IF NOT EXISTS idx_visitors_local_time ON visitors(local_time);
      CREATE INDEX IF NOT EXISTS idx_daily_day_store ON dashboard_daily(day, store_id);
      CREATE INDEX IF NOT EXISTS idx_hourly_day_store_hour ON dashboard_hourly(day, store_id, hour);
    `);

    // Preenche day/local_time/hour se estiver null
    await pool.query(`UPDATE visitors SET day = DATE(timestamp) WHERE day IS NULL`);
    await pool.query(`UPDATE visitors SET local_time = (to_char((timestamp + INTERVAL '${TIMEZONE_OFFSET_HOURS} hour'), 'HH24:MI:SS'))::time WHERE local_time IS NULL`);
    await pool.query(`UPDATE visitors SET hour = EXTRACT(HOUR FROM local_time::time) WHERE hour IS NULL AND local_time IS NOT NULL`);

    return res.status(200).json({ success: true, message: "Tabelas/índices OK e dados preenchidos" });
  } catch (e) {
    console.error("❌ ensureIndexes:", e?.message || String(e));
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

// ===============================
// 14) BACKFILL local_time (mantido)
// ===============================
async function backfillLocalTime(req, res) {
  try {
    const s = String(req.query.start_date || isoDay());
    const e = String(req.query.end_date || s);
    const storeId = String(req.query.store_id || "");
    const batch = Math.max(50, Math.min(2000, parseInt(String(req.query.batch || "500"), 10) || 500));
    const maxBatches = Math.max(1, Math.min(50, parseInt(String(req.query.max_batches || "20"), 10) || 20));

    const start = new Date(s + "T00:00:00Z");
    const end = new Date(e + "T00:00:00Z");

    const days = [];
    for (let d = new Date(start); d <= end && days.length < 7; d = new Date(d.getTime() + 86400000)) {
      days.push(d.toISOString().slice(0, 10));
    }

    let total = 0;
    for (const day of days) {
      let loops = 0;
      while (loops < maxBatches) {
        const where =
          storeId && storeId !== "all"
            ? `day = $1 AND store_id = $2 AND timestamp IS NOT NULL`
            : `day = $1 AND timestamp IS NOT NULL`;

        const params = storeId && storeId !== "all" ? [day, storeId] : [day];

        const sql = `
          UPDATE visitors
          SET
            local_time = (to_char((timestamp + INTERVAL '${TIMEZONE_OFFSET_HOURS} hour'), 'HH24:MI:SS'))::time,
            hour = EXTRACT(HOUR FROM (to_char((timestamp + INTERVAL '${TIMEZONE_OFFSET_HOURS} hour'), 'HH24:MI:SS'))::time)
          WHERE ctid IN (
            SELECT ctid
            FROM visitors
            WHERE ${where} AND local_time IS NULL
            LIMIT ${batch}
          )
        `;

        const upd = await pool.query(sql, params);
        if (!upd.rowCount) break;

        total += upd.rowCount || 0;
        loops++;
      }
    }

    return res.status(200).json({
      success: true,
      updated: total,
      batch,
      max_batches: maxBatches,
      processed_days: days,
    });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
}

// ===============================
// Helpers: empty
// ===============================
function createEmptySummary() {
  return {
    success: true,
    totalVisitors: 0,
    totalMale: 0,
    totalFemale: 0,
    averageAge: 0,
    visitsByDay: {
      Sunday: 0,
      Monday: 0,
      Tuesday: 0,
      Wednesday: 0,
      Thursday: 0,
      Friday: 0,
      Saturday: 0,
    },
    byAgeGroup: { "18-25": 0, "26-35": 0, "36-45": 0, "46-60": 0, "60+": 0 },
    byAgeGender: createEmptyAgeGender(),
    byHour: createEmptyHourlyData().byHour,
    byGenderHour: createEmptyHourlyData().byGenderHour,
    source: "empty_fallback",
  };
}

function createEmptyHourlyData() {
  const byHour = {};
  const byGenderHour = { male: {}, female: {} };
  for (let h = 0; h < 24; h++) {
    byHour[h] = 0;
    byGenderHour.male[h] = 0;
    byGenderHour.female[h] = 0;
  }
  return { byHour, byGenderHour };
}

function createEmptyAgeGender() {
  return {
    "<20": { male: 0, female: 0 },
    "20-29": { male: 0, female: 0 },
    "30-45": { male: 0, female: 0 },
    ">45": { male: 0, female: 0 },
  };
}
