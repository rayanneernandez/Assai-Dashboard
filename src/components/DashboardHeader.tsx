import { LogOut, Menu } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { Button } from "./ui/button";
import { useToast } from "@/hooks/use-toast";

interface DashboardHeaderProps {
  onToggleSidebar: () => void;
}

export const DashboardHeader = ({ onToggleSidebar }: DashboardHeaderProps) => {
  const navigate = useNavigate();
  const { toast } = useToast();

  const handleLogout = () => {
    toast({
      title: "Logout realizado",
      description: "Até logo!",
    });
    navigate("/login");
  };

  return (
    <header className="bg-primary text-primary-foreground shadow-md">
      <div className="px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            size="icon"
            onClick={onToggleSidebar}
            className="text-white hover:bg-white/10"
          >
            <Menu className="w-6 h-6" />
          </Button>
          <div className="w-12 h-12 bg-white rounded-md flex items-center justify-center">
            <span className="text-primary font-bold text-lg">A</span>
          </div>
          <div>
            <h1 className="text-xl font-bold">Assaí Atacadista</h1>
            <p className="text-sm opacity-90">Dashboard de Análise</p>
          </div>
        </div>
        <Button 
          variant="outline" 
          className="bg-transparent border-white text-white hover:bg-white/10"
          onClick={handleLogout}
        >
          <LogOut className="w-4 h-4 mr-2" />
          Sair
        </Button>
      </div>
    </header>
  );
};
