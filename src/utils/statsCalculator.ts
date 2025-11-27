import { Visitor, VisitorStats } from "@/types/api";

const DAYS_ORDER = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"];

export const calculateStats = (visitors: Visitor[]): VisitorStats => {
  const stats: VisitorStats = {
    total: visitors.length,
    men: 0,
    women: 0,
    averageAge: 0,
    byDayOfWeek: {},
    byAgeGroup: {
      "18-25": 0,
      "26-35": 0,
      "36-45": 0,
      "46-60": 0,
      "60+": 0,
    },
    byHour: {},
    byGenderHour: {
      male: {},
      female: {},
    },
  };

  if (visitors.length === 0) return stats;

  let totalAge = 0;

  visitors.forEach((visitor) => {
    // Gender count
    if (visitor.gender === "M") stats.men++;
    else if (visitor.gender === "F") stats.women++;

    // Age calculation
    totalAge += visitor.age || 0;

    // Age group distribution
    const age = visitor.age || 0;
    if (age >= 18 && age <= 25) stats.byAgeGroup["18-25"]++;
    else if (age >= 26 && age <= 35) stats.byAgeGroup["26-35"]++;
    else if (age >= 36 && age <= 45) stats.byAgeGroup["36-45"]++;
    else if (age >= 46 && age <= 60) stats.byAgeGroup["46-60"]++;
    else if (age > 60) stats.byAgeGroup["60+"]++;

    // Day of week distribution
    if (visitor.timestamp) {
      const date = new Date(visitor.timestamp);
      const dayIndex = date.getDay();
      const dayName = DAYS_ORDER[dayIndex === 0 ? 6 : dayIndex - 1];
      stats.byDayOfWeek[dayName] = (stats.byDayOfWeek[dayName] || 0) + 1;
    }

    // Hourly distribution
    if (visitor.timestamp) {
      const hour = new Date(visitor.timestamp).getHours();
      stats.byHour[hour] = (stats.byHour[hour] || 0) + 1;
      
      if (visitor.gender === "M") {
        stats.byGenderHour.male[hour] = (stats.byGenderHour.male[hour] || 0) + 1;
      } else if (visitor.gender === "F") {
        stats.byGenderHour.female[hour] = (stats.byGenderHour.female[hour] || 0) + 1;
      }
    }
  });

  stats.averageAge = visitors.length > 0 ? Math.round(totalAge / visitors.length) : 0;

  return stats;
};
