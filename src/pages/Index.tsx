import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Users, Calendar as CalendarIcon } from "lucide-react";
import { DashboardHeader } from "@/components/DashboardHeader";
import { DashboardSidebar } from "@/components/DashboardSidebar";
import { DashboardFilters } from "@/components/DashboardFilters";
import { StatCard } from "@/components/StatCard";
import { ChatAssistant } from "@/components/ChatAssistant";
import { Card } from "@/components/ui/card";
import { toast } from "@/components/ui/use-toast";


import { fetchDevices, fetchVisitors } from "@/services/api";
import { Device, Visitor, VisitorStats } from "@/types/api";
import { calculateStats } from "@/utils/statsCalculator";
import backend from "@/services/backend";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
  LineChart,
  Line,
} from "recharts";

const IconMale = (props: any) => (
  <svg viewBox="0 0 24 24" {...props}>
    <circle cx="12" cy="6" r="2" fill="currentColor" />
    <rect x="10" y="9" width="4" height="7" rx="1" fill="currentColor" />
    <rect x="7.5" y="10" width="2" height="5" rx="1" fill="currentColor" />
    <rect x="14.5" y="10" width="2" height="5" rx="1" fill="currentColor" />
    <rect x="10" y="16" width="1.6" height="5" rx="0.8" fill="currentColor" />
    <rect x="12.4" y="16" width="1.6" height="5" rx="0.8" fill="currentColor" />
  </svg>
);

const IconFemale = (props: any) => (
  <svg viewBox="0 0 24 24" {...props}>
    <circle cx="12" cy="6" r="2" fill="currentColor" />
    <path d="M12 9 L16 15 H8 Z" fill="currentColor" />
    <rect x="10.2" y="15" width="1.6" height="5" rx="0.8" fill="currentColor" />
    <rect x="12.2" y="15" width="1.6" height="5" rx="0.8" fill="currentColor" />
    <rect x="7.5" y="10" width="2" height="4" rx="1" fill="currentColor" />
    <rect x="14.5" y="10" width="2" height="4" rx="1" fill="currentColor" />
  </svg>
);

const HourTooltip = ({ label, payload, active }: any) => {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="rounded-md bg-white/95 shadow px-2 py-1 text-sm">
      <div className="font-semibold">{`${label}h`}</div>
      {payload.map((p: any, i: number) => (
        <div key={i} className="text-xs" style={{ color: p.color }}>
          {`${p.name} : ${p.value}`}
        </div>
      ))}
    </div>
  );
};

const Index = () => {
  const makeLocalDateStr = () => { const d = new Date(); d.setMinutes(d.getMinutes() - d.getTimezoneOffset()); return d.toISOString().split("T")[0]; };
  const today = makeLocalDateStr();
  const [selectedDevice, setSelectedDevice] = useState("all");
  const [startDate, setStartDate] = useState(today);
  const [endDate, setEndDate] = useState(today);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [appliedFilters, setAppliedFilters] = useState({
    device: "all",
    start: today,
    end: today,
  });

  const { data: devices = [], error: devicesError } = useQuery<Device[]>({
    queryKey: ["devices"],
    queryFn: fetchDevices,
  });



  const { data: backendStats, isLoading: isBackendLoading, error: backendError } = useQuery<VisitorStats>({
    queryKey: ["backendStats", appliedFilters.device, appliedFilters.start, appliedFilters.end],
    queryFn: () =>
      backend.fetchVisitorStats(
        appliedFilters.device === "all" ? undefined : appliedFilters.device,
        appliedFilters.start,
        appliedFilters.end
      ),
    staleTime: 5_000,
    refetchInterval: 5_000,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
    refetchIntervalInBackground: true,
    refetchOnMount: true,
    placeholderData: (prev) => prev as VisitorStats | undefined,
    retry: 0,
  });

  const { data: visitors = [], isLoading, error: visitorsError } = useQuery<Visitor[]>({
    queryKey: ["visitors", appliedFilters.device, appliedFilters.start, appliedFilters.end],
    queryFn: () =>
      fetchVisitors(
        appliedFilters.device === "all" ? undefined : appliedFilters.device,
        appliedFilters.start,
        appliedFilters.end
      ),
    enabled: true,
    retry: 0,
    staleTime: 5_000,
  });

  const computed = calculateStats(visitors);
  const stats: VisitorStats = backendStats
    ? { ...computed,
        total: backendStats.total,
        men: backendStats.men,
        women: backendStats.women,
        averageAge: backendStats.averageAge,
        byDayOfWeek: backendStats.byDayOfWeek || computed.byDayOfWeek,
        byAgeGroup: backendStats.byAgeGroup || computed.byAgeGroup,
        byHour: backendStats.byHour || computed.byHour,
        byGenderHour: backendStats.byGenderHour || computed.byGenderHour,
      }
    : computed;

  useEffect(() => {
    if (devicesError) toast({ title: "Erro ao buscar lojas", description: String(devicesError) });
  }, [devicesError]);

  useEffect(() => {
    if (visitorsError) toast({ title: "Erro ao buscar visitantes", description: String(visitorsError) });
  }, [visitorsError]);

  useEffect(() => {
    if (backendError) {
      const msg = String(backendError);
      if (!/Failed to fetch/i.test(msg)) {
        toast({ title: "Erro ao buscar stats do backend", description: msg });
      }
    }
  }, [backendError]);

  const handleApplyFilters = () => {
    setAppliedFilters({
      device: selectedDevice,
      start: startDate,
      end: endDate,
    });
  };

  useEffect(() => {
    setAppliedFilters({ device: selectedDevice, start: startDate, end: endDate });
  }, [selectedDevice, startDate, endDate]);

  // Prepare chart data
  const dayOrder = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"];
  const dayOfWeekData = dayOrder.map((day) => ({
    day,
    visitantes: stats.byDayOfWeek[day] || 0,
  }));

  const genderData = [
    { name: "Masculino", value: stats.men, color: "#e73c3cff" },
    { name: "Feminino", value: stats.women, color: "#e8e419ff" },
  ];

  const ageGroupData = Object.entries(stats.byAgeGroup).map(([group, count]) => ({
    faixa: group,
    visitantes: count,
  }));

  const hourlyData = Array.from({ length: 24 }, (_, i) => ({
    hora: i,
    visitantes: stats.byHour[i] || 0,
  }));

  const genderHourlyData = Array.from({ length: 24 }, (_, i) => ({
    hora: i,
    masculino: stats.byGenderHour.male[i] || 0,
    feminino: stats.byGenderHour.female[i] || 0,
  }));

  const ageGenderPercentData = (() => {
    const ag = (backendStats as any)?.byAgeGender;
    if (!ag) return [] as Array<{ faixa: string; masculino: number; feminino: number }>;
    const keys = ['<20','20-29','30-45','>45'];
    return keys.map((k) => {
      const m = Number((ag[k]?.male) || 0);
      const f = Number((ag[k]?.female) || 0);
      const sum = m + f;
      return { faixa: k, masculino: sum ? Math.round((m / sum) * 100) : 0, feminino: sum ? Math.round((f / sum) * 100) : 0 };
    });
  })();

  return (
    <div className="min-h-screen bg-background flex">
      <DashboardSidebar isOpen={sidebarOpen} />
      
      <div className="flex-1">
        <DashboardHeader onToggleSidebar={() => setSidebarOpen(!sidebarOpen)} />
        
        <main className="p-6">
          <DashboardFilters
            devices={devices}
            selectedDevice={selectedDevice}
            startDate={startDate}
            endDate={endDate}
            onDeviceChange={setSelectedDevice}
            onStartDateChange={setStartDate}
            onEndDateChange={setEndDate}
            onApplyFilters={handleApplyFilters}
          />


              {/* Stats Cards */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
                <StatCard
                  title="Total de Visitantes"
                  value={backendStats ? backendStats.total : "—"}
                  icon={Users}
                  colorClass="bg-stat-visitors"
                />
                <StatCard
                  title="Total de Homens"
                  value={backendStats ? backendStats.men : "—"}
                  icon={IconMale}
                  colorClass="bg-stat-men"
                />
                <StatCard
                  title="Total de Mulheres"
                  value={backendStats ? backendStats.women : "—"}
                  icon={IconFemale}
                  colorClass="bg-stat-women"
                />
                <StatCard
                  title="Média de Idade"
                  value={backendStats ? `${backendStats.averageAge} anos` : "—"}
                  icon={CalendarIcon}
                  colorClass="bg-stat-age"
                />
              </div>

              {/* Charts Row 1 */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
                <Card className="p-6">
                  <h3 className="text-lg font-semibold text-primary mb-4">
                    Visitas por Dia da Semana
                  </h3>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={dayOfWeekData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="day" tickFormatter={(v) => String(v)} />
                      <YAxis />
                      <Tooltip labelFormatter={(label) => String(label)} />
                      <Bar dataKey="visitantes" fill="#0047BB" />
                    </BarChart>
                  </ResponsiveContainer>
                </Card>

                <Card className="p-6">
                  <h3 className="text-lg font-semibold text-primary mb-4">
                    Distribuição por Gênero
                  </h3>
                  <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                      <Pie
                        data={genderData}
                        cx="50%"
                        cy="50%"
                        labelLine={false}
                        label={({ name, percent }) =>
                          `${name}: ${(percent * 100).toFixed(1)}%`
                        }
                        outerRadius={100}
                        fill="#8884d8"
                        dataKey="value"
                      >
                        {genderData.map((entry, index) => (
                          <Cell key={`cell-${index}`} fill={entry.color} />
                        ))}
                      </Pie>
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                </Card>
              </div>

              {/* Charts Row 2 */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
                <Card className="p-6">
                  <h3 className="text-lg font-semibold text-primary mb-4">
                    Distribuição por Faixa Etária
                  </h3>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={ageGroupData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="faixa" tickFormatter={(v) => String(v)} label={{ value: "Faixa etária", position: "insideBottom", offset: -5 }} />
                      <YAxis label={{ value: "Visitantes", angle: -90, position: "insideLeft" }} />
                      <Tooltip labelFormatter={(label) => String(label)} />
                      <Bar dataKey="visitantes" fill="#0047BB" />
                    </BarChart>
                  </ResponsiveContainer>
                </Card>

                <Card className="p-6">
                  <h3 className="text-lg font-semibold text-primary mb-4">
                    Fluxo de Visitantes por Horário
                  </h3>
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={hourlyData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="hora" ticks={Array.from({ length: 24 }, (_, i) => i)} tickFormatter={(v) => `${v}h`} label={{ value: "Horário (h)", position: "insideBottom", offset: -5 }} />
                      <YAxis label={{ value: "Número de Visitantes", angle: -90, position: "insideLeft" }} />
                      <Tooltip content={<HourTooltip />} />
                      <Line
                        type="monotone"
                        dataKey="visitantes"
                        stroke="#0047BB"
                        strokeWidth={2}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </Card>
              </div>

              {/* Gender by Hour Chart */}
              <Card className="p-6">
                <h3 className="text-lg font-semibold text-primary mb-4">Gênero</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={genderHourlyData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="hora" ticks={Array.from({ length: 24 }, (_, i) => i)} tickFormatter={(v) => `${v}h`} label={{ value: "Horário (h)", position: "insideBottom", offset: -5 }} />
                    <YAxis label={{ value: "Número de Visitantes", angle: -90, position: "insideLeft" }} />
                    <Tooltip content={<HourTooltip />} />
                    <Legend />
                    <Line type="monotone" dataKey="masculino" stroke="#0047BB" strokeWidth={2} name="Masculino" />
                    <Line type="monotone" dataKey="feminino" stroke="#E74C3C" strokeWidth={2} name="Feminino" />
                  </LineChart>
                </ResponsiveContainer>
                {stats.total === 0 && (
                  <p className="text-center text-muted-foreground mt-4">
                    Nenhum dado disponível para o período selecionado
                  </p>
                )}
              </Card>

              {/* Gender × Age Chart */}
              <Card className="p-6 mt-6">
                <h3 className="text-lg font-semibold text-primary mb-4">Gênero & Idade</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={ageGenderPercentData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="faixa" tickFormatter={(v) => String(v)} />
                    <YAxis domain={[0,100]} tickFormatter={(v) => `${v}%`} label={{ value: "Número %", angle: -90, position: "insideLeft" }} />
                    <Tooltip formatter={(value: any, name: any) => [`${value}%`, name]} />
                    <Legend />
                    <Bar dataKey="feminino" name="Feminino" fill="#E74C3C" />
                    <Bar dataKey="masculino" name="Masculino" fill="#0047BB" />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
        </main>
      </div>
      
      <ChatAssistant visitors={visitors} devices={devices} stats={stats} />
    </div>
  );
};

export default Index;
