import dotenv from "dotenv";
import express from "express";
import cors from "cors";
import fetch from "node-fetch";
import bcrypt from "bcryptjs";
import pg from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config({ path: path.join(process.cwd(), "server", ".env") });
dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const { Pool } = pg;
const DATABASE_URL = process.env.DATABASE_URL || process.env.POSTGRES_URL || process.env.POSTGRESQL_URL;
if (!DATABASE_URL) {
  console.error("DATABASE_URL não definido; confira server/.env");
  process.exit(1);
}
const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const schemaPath = path.join(__dirname, "schema.sql");
let schemaSQL = fs.readFileSync(schemaPath, "utf8");
schemaSQL = schemaSQL.replace(/^\uFEFF/, "");
await pool.query(schemaSQL);

app.post("/api/auth/register", async (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) return res.status(400).json({ error: "email e password obrigatórios" });
  const hash = await bcrypt.hash(password, 10);
  try {
    await pool.query("INSERT INTO public.users (email, password_hash) VALUES ($1, $2)", [email, hash]);
    return res.status(201).json({ ok: true });
  } catch (e) {
    if (e.code === "23505") return res.status(409).json({ error: "email já cadastrado" });
    return res.status(500).json({ error: "erro ao cadastrar" });
  }
});

app.post("/api/auth/login", async (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) return res.status(400).json({ error: "email e password obrigatórios" });
  const { rows } = await pool.query("SELECT id, password_hash FROM public.users WHERE email=$1", [email]);
  if (rows.length === 0) return res.status(401).json({ error: "credenciais inválidas" });
  const ok = await bcrypt.compare(password, rows[0].password_hash);
  if (!ok) return res.status(401).json({ error: "credenciais inválidas" });
  return res.json({ ok: true, userId: rows[0].id });
});

function aggregateVisitors(payload) {
  const byAge = { "18-25": 0, "26-35": 0, "36-45": 0, "46-60": 0, "60+": 0 };
  const byWeekday = { monday: 0, tuesday: 0, wednesday: 0, thursday: 0, friday: 0, saturday: 0, sunday: 0 };
  const byHour = {};
  const byGenderHour = { male: {}, female: {} };
  let total = 0, men = 0, women = 0, avgAgeSum = 0, avgAgeCount = 0;
  for (const v of payload) {
    total++;
    const g = v.sex === 1 ? "M" : "F";
    if (g === "M") men++; else women++;
    const age = Number(v.age || 0);
    if (age > 0) { avgAgeSum += age; avgAgeCount++; }
    if (age >= 18 && age <= 25) byAge["18-25"]++; else
    if (age >= 26 && age <= 35) byAge["26-35"]++; else
    if (age >= 36 && age <= 45) byAge["36-45"]++; else
    if (age >= 46 && age <= 60) byAge["46-60"]++; else
    if (age > 60) byAge["60+"]++;
    const ts = v.start || (v.tracks && v.tracks[0] && v.tracks[0].start);
    if (ts) {
      const d = new Date(ts);
      const map = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"];
      const key = map[d.getUTCDay()];
      byWeekday[key] = (byWeekday[key] || 0) + 1;
      const h = d.getUTCHours();
      byHour[h] = (byHour[h] || 0) + 1;
      if (g === "M") byGenderHour.male[h] = (byGenderHour.male[h] || 0) + 1; else byGenderHour.female[h] = (byGenderHour.female[h] || 0) + 1;
    }
  }
  return { total, men, women, avgAgeSum, avgAgeCount, byAge, byWeekday, byHour, byGenderHour };
}

async function fetchDayAllPages(token, day, deviceId) {
  const limit = 500;
  let offset = 0;
  const all = [];
  while (true) {
    const body = {
      start: `${day}T00:00:00Z`,
      end: `${day}T23:59:59Z`,
      limit,
      offset,
      tracks: true,
      face_quality: true,
      glasses: true,
      facial_hair: true,
      hair_color: true,
      hair_type: true,
      headwear: true,
      additional_attributes: ["smile","pitch","yaw","x","y","height"]
    };
    if (deviceId) body.device_id = deviceId;
    const resp = await fetch("https://api.displayforce.ai/public/v1/stats/visitor/list", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-API-Token": token },
      body: JSON.stringify(body)
    });
    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      throw new Error(`API error [${resp.status}] ${resp.statusText} ${t}`);
    }
    const json = await resp.json();
    const payload = json.payload || json.data || [];
    const arr = Array.isArray(payload) ? payload : [];
    all.push(...arr);
    const pg = json.pagination;
    if (!pg || arr.length < limit || (pg.total && all.length >= pg.total)) break;
    offset += limit;
  }
  return all;
}

async function upsertDaily(day, storeId, row) {
  const { rows } = await pool.query(
    "SELECT 1 FROM public.dashboard_daily WHERE day=$1 AND (store_id IS NOT DISTINCT FROM $2)",
    [day, storeId]
  );
  if (rows.length > 0) {
    await pool.query(
      "UPDATE public.dashboard_daily SET total_visitors=$3,male=$4,female=$5,avg_age_sum=$6,avg_age_count=$7,age_18_25=$8,age_26_35=$9,age_36_45=$10,age_46_60=$11,age_60_plus=$12,monday=$13,tuesday=$14,wednesday=$15,thursday=$16,friday=$17,saturday=$18,sunday=$19,updated_at=NOW() WHERE day=$1 AND (store_id IS NOT DISTINCT FROM $2)",
      [
        day, storeId,
        row.total_visitors, row.male, row.female, row.avg_age_sum, row.avg_age_count,
        row.age_18_25, row.age_26_35, row.age_36_45, row.age_46_60, row.age_60_plus,
        row.monday, row.tuesday, row.wednesday, row.thursday, row.friday, row.saturday, row.sunday
      ]
    );
  } else {
    await pool.query(
      "INSERT INTO public.dashboard_daily (day, store_id, total_visitors, male, female, avg_age_sum, avg_age_count, age_18_25, age_26_35, age_36_45, age_46_60, age_60_plus, monday, tuesday, wednesday, thursday, friday, saturday, sunday) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
      [
        day, storeId,
        row.total_visitors, row.male, row.female, row.avg_age_sum, row.avg_age_count,
        row.age_18_25, row.age_26_35, row.age_36_45, row.age_46_60, row.age_60_plus,
        row.monday, row.tuesday, row.wednesday, row.thursday, row.friday, row.saturday, row.sunday
      ]
    );
    console.log(`inserted day=${day} store=${storeId} total=${row.total_visitors}`);
  }
}

async function upsertHourly(day, storeId, byHour, byGenderHour) {
  for (let h = 0; h < 24; h++) {
    const tot = Number(byHour?.[h] || 0);
    const m = Number(byGenderHour?.male?.[h] || 0);
    const f = Number(byGenderHour?.female?.[h] || 0);
    await pool.query(
      "INSERT INTO public.dashboard_hourly (day, store_id, hour, total, male, female) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (day, store_id, hour) DO UPDATE SET total=$4, male=$5, female=$6",
      [day, storeId, h, tot, m, f]
    );
  }
}

async function insertVisitors(items) {
  if (!items || items.length === 0) return;
  const q = `INSERT INTO public.visitors (visitor_id, day_date, timestamp, store_id, store_name, gender, age, day_of_week, smile)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`;
  for (const i of items) {
    const day = String(i.timestamp).slice(0, 10);
    await pool.query(q, [
      i.visitor_id,
      day,
      i.timestamp,
      i.store_id,
      i.store_name || null,
      i.gender,
      i.age,
      i.day_of_week,
      i.smile,
    ]);
  }
}

app.get("/api/stats/visitors", async (req, res) => {
  try {
    const token = process.env.DISPLAYFORCE_TOKEN;
    const { start, end, deviceId } = req.query;
    if (!start || !end) return res.status(400).json({ error: "start e end YYYY-MM-DD são obrigatórios" });
    const storeId = deviceId ? String(deviceId) : "all";

    const days = [];
    let d = new Date(`${start}T00:00:00Z`);
    const endD = new Date(`${end}T00:00:00Z`);
    while (d <= endD) {
      days.push(d.toISOString().slice(0, 10));
      d = new Date(d.getTime() + 86400000);
    }

    const { rows: cached } = await pool.query(
      "SELECT * FROM public.dashboard_daily WHERE day = ANY($1) AND (store_id IS NOT DISTINCT FROM $2)",
      [days, storeId]
    );
    const cachedMap = new Map(
      cached.map((r) => {
        const dv = r.day;
        const dayStr = typeof dv === "string" ? dv.slice(0, 10) : new Date(dv).toISOString().slice(0, 10);
        return [dayStr, r];
      })
    );

    const agg = {
      total: 0, men: 0, women: 0, averageAge: 0,
      byDayOfWeek: { Seg:0, Ter:0, Qua:0, Qui:0, Sex:0, Sáb:0, Dom:0 },
      byAgeGroup: { "18-25":0, "26-35":0, "36-45":0, "46-60":0, "60+":0 },
      byHour: {},
      byGenderHour: { male: {}, female: {} },
    };
    let avgSum = 0;
    let avgCount = 0;

    for (const day of days) {
      let row = cachedMap.get(day);
      const isToday = day === new Date().toISOString().slice(0,10);
      const isStale = isToday && row && row.updated_at && (Date.now() - new Date(row.updated_at).getTime() > 5 * 60 * 1000);
      if (!row) {
        await refreshDayForStore(day, storeId);
        const { rows: r2 } = await pool.query(
          "SELECT * FROM public.dashboard_daily WHERE day=$1 AND (store_id IS NOT DISTINCT FROM $2)",
          [day, storeId]
        );
        row = r2[0];
      } else if (isStale) {
        setImmediate(() => {
          refreshDayForStore(day, storeId).catch((e) => console.error("bg refresh error", e));
        });
      }
      agg.total += Number(row?.total_visitors || 0);
      agg.men += Number(row?.male || 0);
      agg.women += Number(row?.female || 0);
      avgSum += Number(row?.avg_age_sum || 0);
      avgCount += Number(row?.avg_age_count || 0);
      agg.byAgeGroup["18-25"] += Number(row?.age_18_25 || 0);
      agg.byAgeGroup["26-35"] += Number(row?.age_26_35 || 0);
      agg.byAgeGroup["36-45"] += Number(row?.age_36_45 || 0);
      agg.byAgeGroup["46-60"] += Number(row?.age_46_60 || 0);
      agg.byAgeGroup["60+"] += Number(row?.age_60_plus || 0);
      agg.byDayOfWeek["Seg"] += Number(row?.monday || 0);
      agg.byDayOfWeek["Ter"] += Number(row?.tuesday || 0);
      agg.byDayOfWeek["Qua"] += Number(row?.wednesday || 0);
      agg.byDayOfWeek["Qui"] += Number(row?.thursday || 0);
      agg.byDayOfWeek["Sex"] += Number(row?.friday || 0);
      agg.byDayOfWeek["Sáb"] += Number(row?.saturday || 0);
      agg.byDayOfWeek["Dom"] += Number(row?.sunday || 0);
      const { rows: hourly } = await pool.query(
        "SELECT hour, total, male, female FROM public.dashboard_hourly WHERE day=$1 AND (store_id IS NOT DISTINCT FROM $2)",
        [day, storeId]
      );
      if (hourly.length > 0) {
        for (const r of hourly) {
          const h = Number(r.hour);
          agg.byHour[h] = (agg.byHour[h] || 0) + Number(r.total || 0);
          agg.byGenderHour.male[h] = (agg.byGenderHour.male[h] || 0) + Number(r.male || 0);
          agg.byGenderHour.female[h] = (agg.byGenderHour.female[h] || 0) + Number(r.female || 0);
        }
      }
    }

    agg.averageAge = avgCount ? Math.round(avgSum / avgCount) : 0;
    return res.json(agg);
  } catch (e) {
    return res.status(500).json({ error: String(e.message || e) });
  }
});

async function refreshDayForStore(day, storeId) {
  const token = process.env.DISPLAYFORCE_TOKEN;
  const payload = await fetchDayAllPages(token, day, storeId === "all" ? undefined : storeId);
  const a = aggregateVisitors(payload);
  const weekdayRow = {
    monday: a.byWeekday.monday || 0,
    tuesday: a.byWeekday.tuesday || 0,
    wednesday: a.byWeekday.wednesday || 0,
    thursday: a.byWeekday.thursday || 0,
    friday: a.byWeekday.friday || 0,
    saturday: a.byWeekday.saturday || 0,
    sunday: a.byWeekday.sunday || 0
  };
  const toSave = {
    total_visitors: a.total,
    male: a.men,
    female: a.women,
    avg_age_sum: a.avgAgeSum,
    avg_age_count: a.avgAgeCount,
    age_18_25: a.byAge["18-25"] || 0,
    age_26_35: a.byAge["26-35"] || 0,
    age_36_45: a.byAge["36-45"] || 0,
    age_46_60: a.byAge["46-60"] || 0,
    age_60_plus: a.byAge["60+"] || 0,
    ...weekdayRow
  };
  await upsertDaily(day, storeId, toSave);
  await upsertHourly(day, storeId, a.byHour, a.byGenderHour);

  const mapPt = ["Dom","Seg","Ter","Qua","Qui","Sex","Sáb"];
  const items = payload.map((v) => {
    const ts = String(v.start ?? v.tracks?.[0]?.start ?? new Date().toISOString());
    const d = new Date(ts);
    const di = d.getUTCDay();
    const dayOfWeek = mapPt[di];
    const smileRaw = v.smile ?? v.additional_attributes?.smile ?? "";
    const smile = String(smileRaw).toLowerCase() === "yes";
    return {
      visitor_id: String(v.visitor_id ?? v.session_id ?? v.id ?? (v.tracks?.[0]?.id ?? "")),
      timestamp: ts,
      store_id: String(v.tracks?.[0]?.device_id ?? (Array.isArray(v.devices) ? v.devices[0] : "")),
      store_name: String(v.store_name ?? ""),
      gender: (v.sex === 1 ? "M" : "F"),
      age: Number(v.age ?? 0),
      day_of_week: dayOfWeek,
      smile,
      day: ts.slice(0,10),
    };
  });
  await insertVisitors(items);
  console.log(`refreshed day=${day} store=${storeId} total=${toSave.total_visitors}`);
}

async function scheduleBackfill(daysBack = 7) {
  try {
    const today = new Date().toISOString().slice(0,10);
    const start = new Date(new Date(`${today}T00:00:00Z`).getTime() - daysBack * 86400000);
    const days = [];
    let d = start;
    const endD = new Date(`${today}T00:00:00Z`);
    while (d <= endD) {
      days.push(d.toISOString().slice(0,10));
      d = new Date(d.getTime() + 86400000);
    }
    for (const day of days) {
      await refreshDayForStore(day, "all");
    }
    console.log(`backfill completed for ${days.length} days`);
  } catch (e) {
    console.error("backfill error", e);
  }
}

function scheduleRefresh() {
  const run = async () => {
    const day = new Date().toISOString().slice(0,10);
    await refreshDayForStore(day, "all");
  };
  run().catch((e) => console.error("refresh error", e));
  setInterval(() => run().catch((e) => console.error("refresh error", e)), 60_000);
}

scheduleBackfill(7);
scheduleRefresh();

app.get("/api/visitors/list", async (req, res) => {
  try {
    const { start, end, deviceId, page = "1", pageSize = "40" } = req.query;
    if (!start || !end) return res.status(400).json({ error: "start e end YYYY-MM-DD são obrigatórios" });
    
    const p = Math.max(1, parseInt(String(page)) || 1);
    const ps = Math.min(1000, Math.max(1, parseInt(String(pageSize)) || 40));
    const offset = (p - 1) * ps;
    
    let baseWhere = "day >= $1 AND day <= $2";
    const queryParams = [start, end];
    if (deviceId && deviceId !== "all") {
      baseWhere += " AND store_id IS NOT DISTINCT FROM $3";
      queryParams.push(String(deviceId));
    }
    
    const countSql = `SELECT COUNT(*)::int AS total FROM public.visitors WHERE ${baseWhere}`;
    const { rows: cRows } = await pool.query(countSql, queryParams);
    const total = cRows[0]?.total ?? 0;
    
    let listSql = `SELECT visitor_id, day, timestamp, store_id, store_name, gender, age, day_of_week, smile FROM public.visitors WHERE ${baseWhere} ORDER BY timestamp DESC`;
    const params2 = [...queryParams];
    if (queryParams.length === 2) {
      listSql += " LIMIT $3 OFFSET $4";
      params2.push(ps, offset);
    } else {
      listSql += " LIMIT $4 OFFSET $5";
      params2.push(ps, offset);
    }
    
    const { rows } = await pool.query(listSql, params2);
    
    return res.json({ 
      items: rows, 
      total, 
      page: p, 
      pageSize: ps,
      totalPages: Math.ceil(total / ps)
    });
  } catch (e) {
    console.error("Error in /api/visitors/list:", e.message);
    console.error("Stack trace:", e.stack);
    return res.status(500).json({ 
      error: "Erro interno no servidor",
      details: e.message 
    });
  }
});

app.get("/api/admin/refresh", async (req, res) => {
  try {
    const { start, end, deviceId } = req.query;
    const storeId = deviceId ? String(deviceId) : "all";
    const s = typeof start === "string" && start ? start : new Date().toISOString().slice(0,10);
    const e = typeof end === "string" && end ? end : s;
    const days = [];
    let d = new Date(`${s}T00:00:00Z`);
    const endD = new Date(`${e}T00:00:00Z`);
    while (d <= endD) {
      days.push(d.toISOString().slice(0, 10));
      d = new Date(d.getTime() + 86400000);
    }
    for (const day of days) await refreshDayForStore(day, storeId);
    return res.json({ ok: true, days: days.length, storeId });
  } catch (e) {
    return res.status(500).json({ error: String(e.message || e) });
  }
});

const port = process.env.PORT || 3001;
app.listen(port, () => {
  console.log(`Backend running on http://localhost:${port}`);
});