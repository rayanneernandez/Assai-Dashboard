import type { VisitorStats, VisitorsPage } from "@/types/api";

const BACKEND_URL = (import.meta as any).env?.VITE_BACKEND_URL || "";

export async function fetchVisitorStats(deviceId?: string, start?: string, end?: string): Promise<VisitorStats> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    if (BACKEND_URL) {
      try {
        const params = new URLSearchParams();
        if (start) params.set("start", start);
        if (end) params.set("end", end);
        if (deviceId) params.set("deviceId", deviceId);
        const resp = await fetch(`${BACKEND_URL}/stats/visitors?${params.toString()}`, { signal: controller.signal });
        if (!resp.ok) throw new Error(`Backend error [${resp.status}] ${await resp.text()}`);
        return await resp.json();
      } catch {}
    }
    const base = typeof window !== "undefined" ? window.location.origin : "";
    const params = new URLSearchParams();
    params.set("endpoint", "summary");
    if (start) params.set("start_date", start);
    if (end) params.set("end_date", end);
    if (deviceId) params.set("store_id", deviceId);
    try {
      const today = new Date().toISOString().slice(0,10);
      const effStart = start || today;
      const effEnd = end || effStart;
      const startD = new Date(effStart + "T00:00:00Z");
      const endD = new Date(effEnd + "T00:00:00Z");
      const rangeDays = Math.max(1, Math.round((endD.getTime() - startD.getTime())/86400000) + 1);
      const isTodayOnly = effStart === today && effEnd === today;
      if (isTodayOnly || (rangeDays <= 7 && Number.isFinite(rangeDays))) {
        const rf = new URLSearchParams();
        rf.set("endpoint", "refresh");
        rf.set("start_date", effStart);
        rf.set("end_date", effEnd);
        rf.set("store_id", deviceId && deviceId !== "all" ? deviceId : "all");
        fetch(`${base}/api/assai/dashboard?${rf.toString()}`).catch(() => {});
      }
    } catch {}
    let resp = await fetch(`${base}/api/assai/dashboard?${params.toString()}`, { signal: controller.signal });
    if (!resp.ok) throw new Error(`Backend error [${resp.status}] ${await resp.text()}`);
    let json = await resp.json();
    const today = new Date().toISOString().slice(0,10);
    const effStart = start || today;
    const effEnd = end || effStart;
    const startD = new Date(effStart + "T00:00:00Z");
    const endD = new Date(effEnd + "T00:00:00Z");
    const rangeDays = Math.max(1, Math.round((endD.getTime() - startD.getTime())/86400000) + 1);
    const tot = Number((json as any).totalVisitors ?? 0);
    const isFallback = Boolean((json as any).isFallback);
    const byHourEmpty = Object.keys((json as any).byHour ?? {}).length === 0;
    if (rangeDays <= 3 && (tot === 0 || isFallback || byHourEmpty)) {
      params.set("source", "displayforce");
      resp = await fetch(`${base}/api/assai/dashboard?${params.toString()}`, { signal: controller.signal });
      if (resp.ok) json = await resp.json();
    }
    const visitsByDay = (json as any).visitsByDay ?? {};
    const toPt: Record<string, string> = { Sunday: "Dom", Monday: "Seg", Tuesday: "Ter", Wednesday: "Qua", Thursday: "Qui", Friday: "Sex", Saturday: "Sáb" };
    const byDayOfWeek: Record<string, number> = {};
    Object.entries(toPt).forEach(([en, pt]) => { byDayOfWeek[pt] = Number((visitsByDay as any)[en] ?? 0); });
    return {
      total: Number((json as any).totalVisitors ?? 0),
      men: Number((json as any).totalMale ?? 0),
      women: Number((json as any).totalFemale ?? 0),
      averageAge: Number((json as any).averageAge ?? 0),
      byDayOfWeek,
      byAgeGroup: (json as any).byAgeGroup ?? {},
      byHour: (json as any).byHour ?? {},
      byGenderHour: (json as any).byGenderHour ?? { male: {}, female: {} },
    };
  } finally {
    clearTimeout(timeout);
  }
}

export async function fetchVisitorsPage(deviceId?: string, start?: string, end?: string, page = 1, pageSize = 40): Promise<VisitorsPage> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    if (BACKEND_URL) {
      try {
        const params = new URLSearchParams();
        if (start) params.set("start", start);
        if (end) params.set("end", end);
        if (deviceId) params.set("deviceId", deviceId);
        params.set("page", String(page));
        params.set("pageSize", String(pageSize));
        const resp = await fetch(`${BACKEND_URL}/visitors/list?${params.toString()}`, { signal: controller.signal });
        if (!resp.ok) throw new Error(`Backend error [${resp.status}] ${await resp.text()}`);
        return await resp.json();
      } catch {}
    }
    const base = typeof window !== "undefined" ? window.location.origin : "";
    const params = new URLSearchParams();
    params.set("endpoint", "visitors");
    if (start) params.set("start_date", start);
    if (end) params.set("end_date", end);
    params.set("store_id", deviceId && deviceId !== "all" ? String(deviceId) : "all");
    const resp = await fetch(`${base}/api/assai/dashboard?${params.toString()}`, { signal: controller.signal });
    if (!resp.ok) throw new Error(`Backend error [${resp.status}] ${await resp.text()}`);
    const json = await resp.json();
    const rows: any[] = Array.isArray((json as any).data) ? (json as any).data : [];
    const total = rows.length;
    const startIdx = Math.max(0, (page - 1) * pageSize);
    const items = rows.slice(startIdx, startIdx + pageSize).map((r) => ({
      visitor_id: String(r.id ?? r.visitor_id ?? ""),
      timestamp: String(r.timestamp ?? new Date().toISOString()),
      store_id: String(r.store_id ?? ""),
      store_name: String(r.store_name ?? ""),
      gender: (String(r.gender ?? "").toLowerCase().startsWith("m") ? "M" : "F") as "M" | "F",
      age: Number(r.age ?? 0),
      day_of_week: String(r.day_of_week ?? ""),
      smile: Boolean(r.smile),
    }));
    return { items, total, page, pageSize };
  } finally {
    clearTimeout(timeout);
  }
}

export default { fetchVisitorStats, fetchVisitorsPage };