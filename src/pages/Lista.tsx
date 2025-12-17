import { useEffect, useRef, useState } from "react";
import { useQuery, keepPreviousData } from "@tanstack/react-query";
import { format } from "date-fns";
import { ptBR } from "date-fns/locale";

import { DashboardHeader } from "@/components/DashboardHeader";
import { DashboardSidebar } from "@/components/DashboardSidebar";
import { ChatAssistant } from "@/components/ChatAssistant";
import { DashboardFilters } from "@/components/DashboardFilters";
import { toast } from "@/components/ui/use-toast";

import { fetchVisitors, fetchDevices } from "@/services/api";
import backend from "@/services/backend";
import { Device, Visitor } from "@/types/api";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from "@/components/ui/select";

import { calculateStats } from "@/utils/statsCalculator";

const Lista = () => {
  // ✅ Ajuste de horário só para exibição na lista
  const LIST_HOUR_SHIFT = 3;
  const toDisplayDate = (ts: unknown) => {
    const d = new Date(String(ts));
    if (isNaN(d.getTime())) return null;
    return new Date(d.getTime() + LIST_HOUR_SHIFT * 60 * 60 * 1000);
  };

  const [sidebarOpen, setSidebarOpen] = useState(true);

  const todayStr = format(new Date(), "yyyy-MM-dd");
  const [selectedDevice, setSelectedDevice] = useState<string>("all");
  const [startDate, setStartDate] = useState(todayStr);
  const [endDate, setEndDate] = useState(todayStr);

  const [appliedFilters, setAppliedFilters] = useState({
    device: "all",
    start: todayStr,
    end: todayStr,
  });

  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  // ✅ Preserva scroll durante refetch para não “pular pro topo”
  const scrollYRef = useRef<number>(0);

  const { data: devices = [], error: devicesError } = useQuery<Device[]>({
    queryKey: ["devices"],
    queryFn: fetchDevices,
    staleTime: 60_000,
  });

  // ⚠️ Mantido (mas desabilitado) porque a página usa o backend paginado
  const { data: visitors = [] } = useQuery<Visitor[]>({
    queryKey: ["visitors", appliedFilters],
    queryFn: () =>
      fetchVisitors(
        appliedFilters.device === "all" ? undefined : appliedFilters.device,
        appliedFilters.start,
        appliedFilters.end
      ),
    enabled: false,
  });

  const {
    data: backendPage,
    isLoading: isBackendLoading,
    isFetching: isBackendFetching,
    error: backendPageError,
  } = useQuery({
    queryKey: ["backendVisitors", appliedFilters, page, pageSize],
    queryFn: () =>
      backend.fetchVisitorsPage(
        appliedFilters.device === "all" ? undefined : appliedFilters.device,
        appliedFilters.start,
        appliedFilters.end,
        page,
        pageSize
      ),
    enabled: true,

    // ✅ CRÍTICO: mantém dados anteriores durante refetch
    placeholderData: keepPreviousData,

    staleTime: 5_000,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
    refetchInterval:
      appliedFilters.start === todayStr && appliedFilters.end === todayStr
        ? 10_000
        : false,
  });

  const stats = calculateStats(visitors);

  useEffect(() => {
    if (devicesError) {
      toast({
        title: "Erro ao buscar lojas",
        description: String(devicesError),
      });
    }
  }, [devicesError]);

  useEffect(() => {
    if (backendPageError) {
      toast({
        title: "Erro ao buscar visitantes",
        description: String(backendPageError),
      });
    }
  }, [backendPageError]);

  const handleApplyFilters = () => {
    setAppliedFilters({
      device: selectedDevice,
      start: startDate,
      end: endDate,
    });
    setPage(1);
  };

  // ✅ NÃO reseta paginação em refetch; só quando o usuário troca filtros locais
  useEffect(() => {
    // mantém aplicado sincronizado (sem forçar page=1 aqui)
    setAppliedFilters((prev) => ({
      ...prev,
      device: selectedDevice,
      start: startDate,
      end: endDate,
    }));
    // não mexe em page aqui
  }, [selectedDevice, startDate, endDate]);

  // ✅ Clamp seguro: só calcula totalPages quando backendPage.total existir
  useEffect(() => {
    if (backendPage?.total == null) return;
    const tp = Math.max(1, Math.ceil(Number(backendPage.total) / pageSize));
    if (page > tp) setPage(tp);
  }, [backendPage?.total, page, pageSize]);

  // ✅ Preserva scroll durante refetch (garante não voltar pro topo)
  useEffect(() => {
    if (typeof window === "undefined") return;

    if (isBackendFetching) {
      scrollYRef.current = window.scrollY;
      return;
    }

    // quando termina o fetch, volta para o mesmo scroll
    window.scrollTo({ top: scrollYRef.current });
  }, [isBackendFetching]);

  const getDeviceName = (deviceId: string) => {
    const device = devices.find((d) => d.id === deviceId);
    return device?.name || deviceId;
  };

  const backendItems =
    backendPage?.items?.map((i: any) => ({
      id: i.visitor_id,
      gender: i.gender,
      age: i.age,
      timestamp: i.timestamp,
      deviceId: i.store_id,
      dayOfWeek: i.day_of_week,
      smile: i.smile,
    })) ?? [];

  const totalCount = Number(backendPage?.total ?? 0);
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const currentPage = Math.min(page, totalPages);

  return (
    <div className="min-h-screen bg-background flex">
      <DashboardSidebar isOpen={sidebarOpen} />

      <div className="flex-1">
        <DashboardHeader onToggleSidebar={() => setSidebarOpen(!sidebarOpen)} />

        <main className="p-6 space-y-6">
          <div>
            <h2 className="text-2xl font-bold text-primary mb-2">
              Lista Completa de Visitantes
            </h2>
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
                    <TableHead>Sorriso</TableHead>
                  </TableRow>
                </TableHeader>

                <TableBody>
                  {/* ✅ Só mostra skeleton no primeiro load (sem dados ainda) */}
                  {isBackendLoading && backendItems.length === 0 ? (
                    Array.from({ length: 10 }).map((_, i) => (
                      <TableRow key={i}>
                        <TableCell>
                          <Skeleton className="h-4 w-32" />
                        </TableCell>
                        <TableCell>
                          <Skeleton className="h-4 w-24" />
                        </TableCell>
                        <TableCell>
                          <Skeleton className="h-4 w-16" />
                        </TableCell>
                        <TableCell>
                          <Skeleton className="h-4 w-12" />
                        </TableCell>
                        <TableCell>
                          <Skeleton className="h-4 w-20" />
                        </TableCell>
                        <TableCell>
                          <Skeleton className="h-4 w-16" />
                        </TableCell>
                      </TableRow>
                    ))
                  ) : backendItems.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={6}
                        className="text-center text-muted-foreground py-8"
                      >
                        Nenhum visitante encontrado no período selecionado
                      </TableCell>
                    </TableRow>
                  ) : (
                    backendItems.map((visitor: any) => (
                      <TableRow key={visitor.id}>
                        <TableCell className="font-medium">
                          {(() => {
                            const d = toDisplayDate(visitor.timestamp);
                            return !d
                              ? String(visitor.timestamp)
                              : format(d, "dd/MM/yyyy HH:mm", { locale: ptBR });
                          })()}
                        </TableCell>

                        <TableCell>{getDeviceName(visitor.deviceId)}</TableCell>

                        <TableCell>
                          <Badge
                            variant={
                              visitor.gender === "M" ? "default" : "secondary"
                            }
                          >
                            {visitor.gender === "M" ? "Masculino" : "Feminino"}
                          </Badge>
                        </TableCell>

                        <TableCell>{visitor.age} anos</TableCell>

                        <TableCell className="capitalize">
                          {visitor.dayOfWeek ||
                            (() => {
                              const d = toDisplayDate(visitor.timestamp);
                              return d
                                ? format(d, "EEEE", { locale: ptBR })
                                : "";
                            })()}
                        </TableCell>

                        <TableCell>
                          <Badge
                            variant={visitor.smile ? "default" : "secondary"}
                          >
                            {visitor.smile ? "Sim" : "Não"}
                          </Badge>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>

            {/* Rodapé + paginação */}
            {backendItems.length > 0 && (
              <div className="p-4 border-t bg-muted/30 flex flex-col md:flex-row md:items-center md:justify-between gap-3">
                <div className="flex items-center gap-3">
                  <p className="text-sm text-muted-foreground">
                    Total de visitantes:{" "}
                    <span className="font-semibold text-foreground">
                      {totalCount}
                    </span>
                  </p>

                  {/* ✅ Indicador leve durante refetch (sem destruir tabela) */}
                  {isBackendFetching && (
                    <span className="text-xs text-muted-foreground">
                      Atualizando…
                    </span>
                  )}
                </div>

                <div className="flex items-center gap-3">
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-muted-foreground">
                      Itens por página
                    </span>
                    <Select
                      value={String(pageSize)}
                      onValueChange={(v) => {
                        setPageSize(Number(v));
                        setPage(1);
                      }}
                    >
                      <SelectTrigger className="w-[100px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="10">10</SelectItem>
                        <SelectItem value="20">20</SelectItem>
                        <SelectItem value="40">40</SelectItem>
                        <SelectItem value="80">80</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      onClick={() => setPage((p) => Math.max(1, p - 1))}
                      disabled={currentPage <= 1}
                    >
                      Anterior
                    </Button>

                    <span className="text-sm text-muted-foreground">
                      Página{" "}
                      <span className="font-semibold text-foreground">
                        {currentPage}
                      </span>{" "}
                      de{" "}
                      <span className="font-semibold text-foreground">
                        {totalPages}
                      </span>
                    </span>

                    <Button
                      variant="outline"
                      onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                      disabled={currentPage >= totalPages}
                    >
                      Próxima
                    </Button>
                  </div>
                </div>
              </div>
            )}
          </Card>
        </main>
      </div>

      <ChatAssistant visitors={visitors} devices={devices} stats={stats} />
    </div>
  );
};

export default Lista;
