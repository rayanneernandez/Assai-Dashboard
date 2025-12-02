import type { VisitorStats, VisitorsPage } from "@/types/api";

const BACKEND_URL = (import.meta as any).env?.VITE_BACKEND_URL ?? "http://localhost:3001/api";

export async function fetchVisitorStats(deviceId?: string, start?: string, end?: string): Promise<VisitorStats> {
  const params = new URLSearchParams();
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  if (deviceId) params.set("deviceId", deviceId);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const resp = await fetch(`${BACKEND_URL}/stats/visitors?${params.toString()}`, { signal: controller.signal });
    if (!resp.ok) throw new Error(`Backend error [${resp.status}] ${await resp.text()}`);
    return await resp.json();
  } finally {
    clearTimeout(timeout);
  }
}

export async function fetchVisitorsPage(deviceId?: string, start?: string, end?: string, page = 1, pageSize = 40): Promise<VisitorsPage> {
  const params = new URLSearchParams();
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  if (deviceId) params.set("deviceId", deviceId);
  params.set("page", String(page));
  params.set("pageSize", String(pageSize));
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const resp = await fetch(`${BACKEND_URL}/visitors/list?${params.toString()}`, { signal: controller.signal });
    if (!resp.ok) throw new Error(`Backend error [${resp.status}] ${await resp.text()}`);
    return await resp.json();
  } finally {
    clearTimeout(timeout);
  }
}

export default { fetchVisitorStats, fetchVisitorsPage };