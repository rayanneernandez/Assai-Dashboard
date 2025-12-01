import { Calendar, Store } from "lucide-react";
import { Button } from "./ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "./ui/select";
import { Input } from "./ui/input";
import { Device } from "@/types/api";

interface DashboardFiltersProps {
  devices: Device[];
  selectedDevice: string;
  startDate: string;
  endDate: string;
  onDeviceChange: (value: string) => void;
  onStartDateChange: (value: string) => void;
  onEndDateChange: (value: string) => void;
  onApplyFilters: () => void;
}

export const DashboardFilters = ({
  devices,
  selectedDevice,
  startDate,
  endDate,
  onDeviceChange,
  onStartDateChange,
  onEndDateChange,
  onApplyFilters,
}: DashboardFiltersProps) => {
  return (
    <div className="bg-card p-6 rounded-lg shadow-md mb-6 border">
      <div className="flex flex-wrap items-end gap-4">
        <div className="flex-1 min-w-[200px]">
          <label className="flex items-center gap-2 text-sm font-medium mb-2 text-primary">
            <Store className="w-4 h-4" />
            Loja
          </label>
          <Select value={selectedDevice} onValueChange={onDeviceChange}>
            <SelectTrigger className="text-primary">
              <SelectValue placeholder="Todas as Lojas" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todas as Lojas</SelectItem>
              {devices.map((device) => (
                <SelectItem key={device.id} value={device.id}>
                  {device.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="flex-1 min-w-[200px]">
          <label className="flex items-center gap-2 text-sm font-medium mb-2 text-primary">
            <Calendar className="w-4 h-4" />
            Período
          </label>
          <div className="flex gap-2">
            <Input
              type="date"
              value={startDate}
              onChange={(e) => onStartDateChange(e.target.value)}
              className="flex-1 text-primary focus-visible:ring-0 focus-visible:ring-offset-0"
            />
            <span className="flex items-center px-2">→</span>
            <Input
              type="date"
              value={endDate}
              onChange={(e) => onEndDateChange(e.target.value)}
              className="flex-1 border-primary text-primary focus-visible:ring-primary"
            />
          </div>
        </div>

        <Button onClick={onApplyFilters} className="bg-primary hover:bg-primary/90">
          Aplicar Filtros
        </Button>
      </div>
    </div>
  );
};
