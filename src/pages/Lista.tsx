import { DashboardHeader } from "@/components/DashboardHeader";
import { DashboardSidebar } from "@/components/DashboardSidebar";

const Lista = () => {
  return (
    <div className="min-h-screen bg-background flex">
      <DashboardSidebar />
      
      <div className="flex-1">
        <DashboardHeader />
        
        <main className="p-6">
          <h2 className="text-2xl font-bold text-primary mb-4">Lista Completa de Visitantes</h2>
          <p className="text-muted-foreground">Em desenvolvimento...</p>
        </main>
      </div>
    </div>
  );
};

export default Lista;
