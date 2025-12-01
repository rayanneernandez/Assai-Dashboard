import { Device, Visitor } from "@/types/api";

const API_BASE_URL = "/api/displayforce";
const API_TOKEN = "4AUH-BX6H-G2RJ-G7PB";

const headers = {
  "X-API-Token": API_TOKEN,
  "Content-Type": "application/json",
};

const AUTH_QUERY = "";

export const fetchDevices = async (): Promise<Device[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/device/list`, { headers, method: "POST", body: JSON.stringify({}) });
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Failed to fetch devices [${response.status}] ${response.statusText} ${body}`);
    }
    const data = await response.json();
    const list = (data as any).payload ?? (data as any).devices ?? (data as any).data ?? data;
    return Array.isArray(list) ? (list as any[]) : [];
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
    const startISO = `${(startDate || new Date().toISOString().split("T")[0])}T00:00:00Z`;
    const endISO = `${(endDate || new Date().toISOString().split("T")[0])}T23:59:59Z`;
    const limit = 500;

    let offset = 0;
    const all: any[] = [];
    // paginação incremental para reduzir tempo de carregamento por requisição
    // e cobrir todo o período
    // interrompe caso a API retorne menos que "limit"
    while (true) {
      const bodyPayload: Record<string, any> = {
        start: startISO,
        end: endISO,
        limit,
        offset,
        tracks: true,
        face_quality: true,
        glasses: true,
        facial_hair: true,
        hair_color: true,
        hair_type: true,
        headwear: true,
        additional_attributes: ["smile", "pitch", "yaw", "x", "y", "height"],
      };
      if (deviceId && deviceId !== "all") bodyPayload.device_id = deviceId;

      const response = await fetch(`${API_BASE_URL}/stats/visitor/list`, {
        method: "POST",
        headers,
        body: JSON.stringify(bodyPayload),
      });
      if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(`Failed to fetch visitors [${response.status}] ${response.statusText} ${body}`);
      }
      const page = await response.json();
      const raw = (page as any).payload ?? (page as any).visitors ?? (page as any).data ?? [];
      const arr: any[] = Array.isArray(raw) ? raw : [];
      all.push(...arr);

      const pg = (page as any).pagination;
      if (!pg || arr.length < limit || (pg.total && all.length >= pg.total)) break;
      offset += limit;
    }

    const DAYS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"];
    return all.map((v) => {
      const ts = String(v.start ?? v.tracks?.[0]?.start ?? new Date().toISOString());
      const d = new Date(ts);
      const di = d.getUTCDay();
      const dayOfWeek = DAYS[di === 0 ? 6 : di - 1];
      const smileRaw = (v as any).smile ?? (v as any).additional_attributes?.smile ?? "";
      const smile = String(smileRaw).toLowerCase() === "yes";
      return {
        id: String(v.visitor_id ?? v.session_id ?? v.id ?? (v.tracks?.[0]?.id ?? "")),
        gender: (v.sex === 1 ? "M" : "F") as "M" | "F",
        age: Number(v.age ?? 0),
        timestamp: ts,
        deviceId: String(v.tracks?.[0]?.device_id ?? (Array.isArray(v.devices) ? v.devices[0] : "")),
        dayOfWeek,
        hour: d.getUTCHours(),
        smile,
      };
    });
  } catch (error) {
    console.error("Error fetching visitors:", error);
    throw error instanceof Error ? error : new Error(String(error));
  }
};
