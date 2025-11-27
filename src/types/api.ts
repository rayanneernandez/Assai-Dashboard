export interface Visitor {
  id: string;
  gender: "M" | "F";
  age: number;
  timestamp: string;
  deviceId: string;
  hour?: number;
  dayOfWeek?: string;
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
