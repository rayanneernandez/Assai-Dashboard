export interface Visitor {
  id: string;
  gender: "M" | "F";
  age: number;
  timestamp: string;
  deviceId: string;
  hour?: number;
  dayOfWeek?: string;
  smile?: boolean;
}

export interface Device {
  id: string;
  name: string;
  location: string;
  status: string;
}

export interface VisitorStats {
  total: number;
  men: number;
  women: number;
  averageAge: number;
  byDayOfWeek: Record<string, number>;
  byAgeGroup: Record<string, number>;
  byHour: Record<number, number>;
  byGenderHour: {
    male: Record<number, number>;
    female: Record<number, number>;
  };
}

export interface VisitorsPage {
  items: Array<{
    visitor_id: string;
    timestamp: string;
    store_id: string;
    store_name?: string;
    gender: "M" | "F";
    age: number;
    day_of_week: string;
    smile: boolean;
  }>;
  total: number;
  page: number;
  pageSize: number;
}
