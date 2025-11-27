import { LayoutDashboard, List } from "lucide-react";
import { NavLink } from "./NavLink";

export const DashboardSidebar = () => {
  return (
    <aside className="w-48 bg-sidebar min-h-screen p-4">
      <div className="mb-8">
        <h2 className="text-sidebar-foreground font-semibold text-lg px-3">Menu</h2>
      </div>
      <nav className="space-y-2">
        <NavLink
          to="/"
          className="flex items-center gap-3 px-3 py-2 text-sidebar-foreground hover:bg-sidebar-accent rounded-md transition-colors"
          activeClassName="bg-sidebar-accent"
        >
          <LayoutDashboard className="w-5 h-5" />
          <span>Dashboard</span>
        </NavLink>
        <NavLink
          to="/lista"
          className="flex items-center gap-3 px-3 py-2 text-sidebar-foreground hover:bg-sidebar-accent rounded-md transition-colors"
          activeClassName="bg-sidebar-accent"
        >
          <List className="w-5 h-5" />
          <span>Ver lista completa</span>
        </NavLink>
      </nav>
    </aside>
  );
};
