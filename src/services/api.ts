import { Device, Visitor } from "@/types/api";

const RAW_BASE = (import.meta as any).env?.VITE_API_URL;
const BASE_TRIM = typeof RAW_BASE === "string" ? RAW_BASE.trim() : "";
const ORIGIN = typeof window !== "undefined" ? window.location.origin : "";
const BASE_NORM = (() => {
  if (!BASE_TRIM) return "";
  const noJs = BASE_TRIM.replace(/dashboard\.js$/i, "");
  const noDash = noJs.replace(/\/dashboard$/i, "");
  const collapsed = noDash.replace(/\/+$/g, "");
  const idx = collapsed.indexOf("/api/assai");
  return idx >= 0 ? collapsed.slice(0, idx + "/api/assai".length) : `${collapsed}/api/assai`;
})();
const API_BASE_URL = ORIGIN ? `${ORIGIN}/api/assai` : (BASE_NORM || "/api/assai");
const API_TOKEN = (import.meta as any).env?.VITE_DISPLAYFORCE_API_TOKEN ?? "4AUH-BX6H-G2RJ-G7PB";

const headers = {
  "X-API-Token": API_TOKEN,
  "Content-Type": "application/json",
};

const AUTH_QUERY = "";

export const fetchDevices = async (): Promise<Device[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/dashboard?endpoint=devices`, { headers: { Accept: "application/json" } });
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Failed to fetch devices [${response.status}] ${response.statusText} ${body}`);
    }
    let data: any;
    try { data = await response.json(); } catch {
      const body = await response.text().catch(() => "");
      throw new SyntaxError(`Devices JSON parse failed: ${body.slice(0,120)}`);
    }
    const list = (data as any).devices ?? (data as any).data ?? [];
    return Array.isArray(list)
      ? (list as any[]).map((d: any) => ({
          id: String(d.id ?? d.device_id ?? ""),
          name: String(d.name ?? d.title ?? "Dispositivo"),
          location: String(d.address?.description ?? d.location ?? ""),
          status: String(d.connection_state ?? d.status ?? "unknown"),
        }))
      : [];
  } catch (error) {
    console.error("Error fetching devices:", error);
    throw error instanceof Error ? error : new Error(String(error));
  }
};

export const fetchVisitors = async (
  deviceId?: string,
  startDate?: string,
  endDate?: string
): Promise<Visitor[]> => {
  try {
    const today = new Date().toISOString().split("T")[0];
    const params = new URLSearchParams();
    params.set("endpoint", "visitors");
    params.set("start_date", startDate || today);
    params.set("end_date", endDate || today);
    if (deviceId && deviceId !== "all") params.set("store_id", deviceId);


    const response = await fetch(`${API_BASE_URL}/dashboard?${params.toString()}`, { headers: { Accept: "application/json" } });
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Failed to fetch visitors [${response.status}] ${response.statusText} ${body}`);
    }
    let json: any;
    try { json = await response.json(); } catch {
      const body = await response.text().catch(() => "");
      throw new SyntaxError(`Visitors JSON parse failed: ${body.slice(0,120)}`);
    }
    const raw = (json as any).data ?? [];

    const mapped = (raw as any[]).map((row: any) => {
      const ts = String(row.timestamp ?? new Date().toISOString());
      const d = new Date(ts);
      const genderStr = String(row.gender ?? "").toLowerCase();
      const gender = genderStr.startsWith("m") ? "M" : "F";
      return {
        id: String(row.id ?? row.visitor_id ?? ""),
        gender: gender as "M" | "F",
        age: Number(row.age ?? 0),
        timestamp: ts,
        deviceId: String(row.store_id ?? ""),
        dayOfWeek: String(row.day_of_week ?? ""),
        hour: d.getHours(),
        smile: Boolean(row.smile),
      } as Visitor;
    });

    const unique = Array.from(new Map(mapped.map((m) => [`${m.id}-${m.timestamp}`, m])).values());
    return unique;
  } catch (error) {
    console.error("Error fetching visitors:", error);
    throw error instanceof Error ? error : new Error(String(error));
  }
};
