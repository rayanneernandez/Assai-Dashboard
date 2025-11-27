import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { ptBR } from "date-fns/locale";
import { DashboardHeader } from "@/components/DashboardHeader";
import { DashboardSidebar } from "@/components/DashboardSidebar";
import { ChatAssistant } from "@/components/ChatAssistant";
import { DashboardFilters } from "@/components/DashboardFilters";
import { fetchVisitors, fetchDevices } from "@/services/api";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";

const Lista = () => {
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [selectedDevice, setSelectedDevice] = useState<string>("all");
  const [startDate, setStartDate] = useState(
    format(new Date(new Date().setDate(new Date().getDate() - 7)), "yyyy-MM-dd")
  );
  const [endDate, setEndDate] = useState(format(new Date(), "yyyy-MM-dd"));
  const [appliedFilters, setAppliedFilters] = useState({
    device: "all",
    start: format(new Date(new Date().setDate(new Date().getDate() - 7)), "yyyy-MM-dd"),
    end: format(new Date(), "yyyy-MM-dd"),
  });

  const { data: devices = [] } = useQuery({
    queryKey: ["devices"],
    queryFn: fetchDevices,
  });

  const { data: visitors = [], isLoading } = useQuery({
    queryKey: ["visitors", appliedFilters],
    queryFn: () =>
      fetchVisitors(
        appliedFilters.device === "all" ? undefined : appliedFilters.device,
        appliedFilters.start,
        appliedFilters.end
      ),
  });

  const handleApplyFilters = () => {
    setAppliedFilters({
      device: selectedDevice,
      start: startDate,
      end: endDate,
    });
  };

  const getDeviceName = (deviceId: string) => {
    const device = devices.find((d) => d.id === deviceId);
    return device?.name || deviceId;
  };

  return (
    <div className="min-h-screen bg-background flex">
      <DashboardSidebar isOpen={sidebarOpen} />
      
      <div className="flex-1">
        <DashboardHeader onToggleSidebar={() => setSidebarOpen(!sidebarOpen)} />
        
        <main className="p-6 space-y-6">
          <div>
            <h2 className="text-2xl font-bold text-primary mb-2">Lista Completa de Visitantes</h2>
            <p className="text-muted-foreground">
              Visualize todos os visitantes registrados no período selecionado
            </p>
          </div>

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

          <Card>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Data/Hora</TableHead>
                    <TableHead>Loja</TableHead>
                    <TableHead>Gênero</TableHead>
                    <TableHead>Idade</TableHead>
                    <TableHead>Dia da Semana</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {isLoading ? (
                    Array.from({ length: 10 }).map((_, i) => (
                      <TableRow key={i}>
                        <TableCell><Skeleton className="h-4 w-32" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-12" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-20" /></TableCell>
                      </TableRow>
                    ))
                  ) : visitors.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                        Nenhum visitante encontrado no período selecionado
                      </TableCell>
                    </TableRow>
                  ) : (
                    visitors.map((visitor) => (
                      <TableRow key={visitor.id}>
                        <TableCell className="font-medium">
                          {format(new Date(visitor.timestamp), "dd/MM/yyyy HH:mm", { locale: ptBR })}
                        </TableCell>
                        <TableCell>{getDeviceName(visitor.deviceId)}</TableCell>
                        <TableCell>
                          <Badge variant={visitor.gender === "M" ? "default" : "secondary"}>
                            {visitor.gender === "M" ? "Masculino" : "Feminino"}
                          </Badge>
                        </TableCell>
                        <TableCell>{visitor.age} anos</TableCell>
                        <TableCell className="capitalize">
                          {visitor.dayOfWeek || format(new Date(visitor.timestamp), "EEEE", { locale: ptBR })}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
            {!isLoading && visitors.length > 0 && (
              <div className="p-4 border-t bg-muted/30">
                <p className="text-sm text-muted-foreground">
                  Total de visitantes: <span className="font-semibold text-foreground">{visitors.length}</span>
                </p>
              </div>
            )}
          </Card>
        </main>
      </div>
      
      <ChatAssistant />
    </div>
  );
};

export default Lista;
