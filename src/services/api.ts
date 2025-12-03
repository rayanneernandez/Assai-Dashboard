import { Device, Visitor } from "@/types/api";

const RAW_BASE = (import.meta as any).env?.VITE_API_URL;
const BASE_TRIM = typeof RAW_BASE === "string" ? RAW_BASE.trim().replace(/\/+$/, "") : "";
const API_BASE_URL = BASE_TRIM
  ? (/\/api\/assai$/.test(BASE_TRIM) ? BASE_TRIM : `${BASE_TRIM}/api/assai`)
  : (typeof window !== "undefined" ? `${window.location.origin}/api/assai` : "/api/assai");
const API_TOKEN = (import.meta as any).env?.VITE_DISPLAYFORCE_API_TOKEN ?? "4AUH-BX6H-G2RJ-G7PB";

const headers = {
  "X-API-Token": API_TOKEN,
  "Content-Type": "application/json",
};

const AUTH_QUERY = "";

export const fetchDevices = async (): Promise<Device[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/dashboard?endpoint=devices`);
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Failed to fetch devices [${response.status}] ${response.statusText} ${body}`);
    }
    const data = await response.json();
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
    params.set("store_id", deviceId && deviceId !== "all" ? deviceId : "all");


    const response = await fetch(`${API_BASE_URL}/dashboard?${params.toString()}`);
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Failed to fetch visitors [${response.status}] ${response.statusText} ${body}`);
    }
    const json = await response.json();
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
        hour: d.getUTCHours(),
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
