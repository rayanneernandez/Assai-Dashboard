import { Device, Visitor } from "@/types/api";

const API_BASE_URL = "https://api.displayforce.ai/public/v1";
const API_TOKEN = "4AUH-BX6H-G2RJ-G7PB";

const headers = {
  Authorization: `Bearer ${API_TOKEN}`,
  "Content-Type": "application/json",
};

export const fetchDevices = async (): Promise<Device[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/device/list`, { headers });
    if (!response.ok) throw new Error("Failed to fetch devices");
    const data = await response.json();
    return data.devices || data || [];
  } catch (error) {
    console.error("Error fetching devices:", error);
    return [];
  }
};

export const fetchVisitors = async (
  deviceId?: string,
  startDate?: string,
  endDate?: string
): Promise<Visitor[]> => {
  try {
    const params = new URLSearchParams();
    if (deviceId) params.append("deviceId", deviceId);
    if (startDate) params.append("startDate", startDate);
    if (endDate) params.append("endDate", endDate);

    const url = `${API_BASE_URL}/stats/visitor/list${params.toString() ? `?${params.toString()}` : ""}`;
    const response = await fetch(url, { headers });
    
    if (!response.ok) throw new Error("Failed to fetch visitors");
    const data = await response.json();
    return data.visitors || data || [];
  } catch (error) {
    console.error("Error fetching visitors:", error);
    return [];
  }
};
