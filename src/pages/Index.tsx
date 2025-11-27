import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Users, UserCheck, UserX, Calendar as CalendarIcon } from "lucide-react";
import { DashboardHeader } from "@/components/DashboardHeader";
import { DashboardSidebar } from "@/components/DashboardSidebar";
import { DashboardFilters } from "@/components/DashboardFilters";
import { StatCard } from "@/components/StatCard";
import { Card } from "@/components/ui/card";
import { fetchDevices, fetchVisitors } from "@/services/api";
import { calculateStats } from "@/utils/statsCalculator";
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

const Index = () => {
  const today = new Date().toISOString().split("T")[0];
  const [selectedDevice, setSelectedDevice] = useState("all");
  const [startDate, setStartDate] = useState(today);
  const [endDate, setEndDate] = useState(today);
  const [appliedFilters, setAppliedFilters] = useState({
    device: "all",
    start: today,
    end: today,
  });

  const { data: devices = [] } = useQuery({
    queryKey: ["devices"],
    queryFn: fetchDevices,
  });

  const { data: visitors = [], isLoading } = useQuery({
    queryKey: ["visitors", appliedFilters.device, appliedFilters.start, appliedFilters.end],
    queryFn: () =>
      fetchVisitors(
        appliedFilters.device === "all" ? undefined : appliedFilters.device,
        appliedFilters.start,
        appliedFilters.end
      ),
  });

  const stats = calculateStats(visitors);

  const handleApplyFilters = () => {
    setAppliedFilters({
      device: selectedDevice,
      start: startDate,
      end: endDate,
    });
  };

  // Prepare chart data
  const dayOfWeekData = Object.entries(stats.byDayOfWeek).map(([day, count]) => ({
    day,
    visitantes: count,
  }));

  const genderData = [
    { name: "Masculino", value: stats.men, color: "#0047BB" },
    { name: "Feminino", value: stats.women, color: "#E74C3C" },
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

  return (
    <div className="min-h-screen bg-background flex">
      <DashboardSidebar />
      
      <div className="flex-1">
        <DashboardHeader />
        
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

          {isLoading ? (
            <div className="text-center py-12">Carregando dados...</div>
          ) : (
            <>
              {/* Stats Cards */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
                <StatCard
                  title="Total de Visitantes"
                  value={stats.total}
                  icon={Users}
                  colorClass="bg-stat-visitors"
                />
                <StatCard
                  title="Total de Homens"
                  value={stats.men}
                  icon={UserCheck}
                  colorClass="bg-stat-men"
                />
                <StatCard
                  title="Total de Mulheres"
                  value={stats.women}
                  icon={UserX}
                  colorClass="bg-stat-women"
                />
                <StatCard
                  title="Média de Idade"
                  value={`${stats.averageAge} anos`}
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
                      <XAxis dataKey="day" />
                      <YAxis />
                      <Tooltip />
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
                      <XAxis dataKey="faixa" label={{ value: "Faixa etária", position: "insideBottom", offset: -5 }} />
                      <YAxis label={{ value: "Visitantes", angle: -90, position: "insideLeft" }} />
                      <Tooltip />
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
                      <XAxis dataKey="hora" label={{ value: "Horário", position: "insideBottom", offset: -5 }} />
                      <YAxis label={{ value: "Número de Visitantes", angle: -90, position: "insideLeft" }} />
                      <Tooltip />
                      <Line
                        type="monotone"
                        dataKey="visitantes"
                        stroke="#0047BB"
                        strokeWidth={2}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                  {stats.total === 0 && (
                    <p className="text-center text-muted-foreground mt-4">
                      Nenhum dado disponível para o período selecionado
                    </p>
                  )}
                </Card>
              </div>

              {/* Gender by Hour Chart */}
              <Card className="p-6">
                <h3 className="text-lg font-semibold text-primary mb-4">Gênero</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={genderHourlyData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="hora" label={{ value: "Horário", position: "insideBottom", offset: -5 }} />
                    <YAxis label={{ value: "Número de Visitantes", angle: -90, position: "insideLeft" }} />
                    <Tooltip />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="masculino"
                      stroke="#0047BB"
                      strokeWidth={2}
                      name="Masculino"
                    />
                    <Line
                      type="monotone"
                      dataKey="feminino"
                      stroke="#E74C3C"
                      strokeWidth={2}
                      name="Feminino"
                    />
                  </LineChart>
                </ResponsiveContainer>
                {stats.total === 0 && (
                  <p className="text-center text-muted-foreground mt-4">
                    Nenhum dado disponível para o período selecionado
                  </p>
                )}
              </Card>
            </>
          )}
        </main>
      </div>
    </div>
  );
};

export default Index;
