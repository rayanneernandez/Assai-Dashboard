import { LucideIcon } from "lucide-react";
import { Card } from "./ui/card";

interface StatCardProps {
  title: string;
  value: string | number;
  icon: LucideIcon;
  colorClass: string;
}

export const StatCard = ({ title, value, icon: Icon, colorClass }: StatCardProps) => {
  return (
    <Card className={`${colorClass} border-none shadow-md p-6 text-white`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium opacity-90 mb-2">{title}</p>
          <p className="text-4xl font-bold">{value}</p>
        </div>
        <Icon className="w-12 h-12 opacity-80" />
      </div>
    </Card>
  );
};
