import { LogOut } from "lucide-react";
import { Button } from "./ui/button";

export const DashboardHeader = () => {
  return (
    <header className="bg-primary text-primary-foreground shadow-md">
      <div className="px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="w-12 h-12 bg-white rounded-md flex items-center justify-center">
            <span className="text-primary font-bold text-lg">A</span>
          </div>
          <div>
            <h1 className="text-xl font-bold">Assaí Atacadista</h1>
            <p className="text-sm opacity-90">Dashboard de Análise</p>
          </div>
        </div>
        <Button variant="outline" className="bg-transparent border-white text-white hover:bg-white/10">
          <LogOut className="w-4 h-4 mr-2" />
          Sair
        </Button>
      </div>
    </header>
  );
};
