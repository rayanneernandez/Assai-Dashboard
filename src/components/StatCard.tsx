import React from "react";
import { Card } from "./ui/card";

interface StatCardProps {
  title: string;
  value: string | number;
  icon: React.ElementType;
  colorClass: string;
}

export const StatCard = ({ title, value, icon: Icon, colorClass }: StatCardProps) => {
  return (
    <Card className={`${colorClass} border-none shadow-md p-4 text-white`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs font-medium opacity-90 mb-1">{title}</p>
          <p className="text-2xl font-bold">{value}</p>
        </div>
        <Icon className="w-8 h-8 opacity-80" />
      </div>
    </Card>
  );
};
