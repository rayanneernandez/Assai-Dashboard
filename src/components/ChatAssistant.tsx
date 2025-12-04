import { useState, useRef, useEffect } from "react";
import { MessageCircle, X, Maximize2, Minimize2, Send } from "lucide-react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Card } from "./ui/card";
import { ScrollArea } from "./ui/scroll-area";
import backend from "@/services/backend";

interface Message {
  role: "user" | "assistant";
  content: string;
}

import { Visitor, Device, VisitorStats } from "@/types/api";

interface ChatAssistantProps {
  visitors: Visitor[];
  devices: Device[];
  stats: VisitorStats;
}

export const ChatAssistant = ({ visitors, devices, stats }: ChatAssistantProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [isMaximized, setIsMaximized] = useState(false);
  const [messages, setMessages] = useState<Message[]>([
    {
      role: "assistant",
      content: "Olá! Sou o assistente virtual do Assaí. Como posso ajudá-lo com as informações do dashboard?",
    },
  ]);
  const [input, setInput] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const generateAnswer = async (q: string) => {
    const text = q.toLowerCase();
    const dateMatch = text.match(/(\d{2})[\/\-](\d{2})[\/\-](\d{4})|(\d{4})-(\d{2})-(\d{2})/);
    const shortMatch = !dateMatch ? text.match(/(^|\s)(\d{1,2})[\/\-](\d{1,2})(?!\d)/) : null;
    let day: string | null = null;
    if (text.includes("hoje")) {
      day = new Date().toISOString().split("T")[0];
    } else if (text.includes("ontem")) {
      const d = new Date(); d.setDate(d.getDate() - 1); day = d.toISOString().split("T")[0];
    } else if (dateMatch) {
      if (dateMatch[1]) {
        const dd = dateMatch[1], mm = dateMatch[2], yyyy = dateMatch[3];
        day = `${yyyy}-${mm}-${dd}`;
      } else {
        const yyyy = dateMatch[4], mm = dateMatch[5], dd = dateMatch[6];
        day = `${yyyy}-${mm}-${dd}`;
      }
    } else if (shortMatch) {
      const now = new Date();
      const yyyy = String(now.getFullYear());
      const dd = String(shortMatch[2]).padStart(2, "0");
      const mm = String(shortMatch[3]).padStart(2, "0");
      day = `${yyyy}-${mm}-${dd}`;
    }
    if (day) {
      try {
        const s = await backend.fetchVisitorStats(undefined, day, day);
        if (text.includes("homem")) return `Total de homens em ${day}: ${s.men}`;
        if (text.includes("mulher")) return `Total de mulheres em ${day}: ${s.women}`;
        const hm = text.match(/(?:às|as)\s*(\d{1,2})/);
        if (hm) {
          const h = parseInt(hm[1]);
          const totalH = s.byHour[h] || 0;
          return `Visitantes às ${h}h em ${day}: ${totalH}`;
        }
        return `Total de visitantes em ${day}: ${s.total}`;
      } catch {
        return `Não consegui consultar o banco para ${day}. Total atual no dashboard: ${stats.total}`;
      }
    }
    if (text.includes("total") && text.includes("visit")) return `Total de visitantes: ${stats.total}`;
    if (text.includes("homem")) return `Total de homens: ${stats.men}`;
    if (text.includes("mulher")) {
      const m = text.match(/(?:às|as)\s*(\d{1,2})/);
      if (m) {
        const h = parseInt(m[1]);
        const f = visitors.filter(v => new Date(v.timestamp).getUTCHours() === h && v.gender === "F").length;
        return `Mulheres às ${h}h: ${f}`;
      }
      return `Total de mulheres: ${stats.women}`;
    }
    if (text.includes("idade") && text.includes("média")) return `Idade média: ${stats.averageAge} anos`;
    const dayMap: Record<string,string> = { seg: "Seg", segunda: "Seg", ter: "Ter", terça: "Ter", qua: "Qua", quarta: "Qua", qui: "Qui", quinta: "Qui", sex: "Sex", sexta: "Sex", sab: "Sáb", sábado: "Sáb", dom: "Dom", domingo: "Dom" };
    for (const k in dayMap) {
      if (text.includes(k)) return `Visitantes em ${dayMap[k]}: ${stats.byDayOfWeek[dayMap[k]] || 0}`;
    }
    const hm = text.match(/(?:às|as)\s*(\d{1,2})/);
    if (hm) {
      const h = parseInt(hm[1]);
      const totalH = visitors.filter(v => new Date(v.timestamp).getUTCHours() === h).length;
      return `Visitantes às ${h}h: ${totalH}`;
    }
    return `Total: ${stats.total} | Homens: ${stats.men} | Mulheres: ${stats.women} | Idade média: ${stats.averageAge} anos`;
  };

  const handleSend = async () => {
    if (!input.trim()) return;

    const userMessage: Message = { role: "user", content: input };
    setMessages((prev) => [...prev, userMessage]);
    const q = input;
    setInput("");
    const reply = await generateAnswer(q);
    const assistantMessage: Message = { role: "assistant", content: reply };
    setMessages((prev) => [...prev, assistantMessage]);
  };

  const handleKeyPress = (e: any) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  if (!isOpen) {
    return (
      <button
        onClick={() => setIsOpen(true)}
        className="fixed bottom-6 right-6 bg-primary hover:bg-primary/90 text-white rounded-full p-4 shadow-lg transition-transform hover:scale-110 z-50"
        aria-label="Abrir assistente"
      >
        <MessageCircle className="w-6 h-6" />
      </button>
    );
  }

  return (
    <Card
      className={`fixed ${
        isMaximized
          ? "inset-4 md:inset-8"
          : "bottom-6 right-6 w-[360px] h-[500px]"
      } shadow-2xl z-50 flex flex-col transition-all duration-300 border-2 border-primary/20`}
    >
      {/* Header */}
      <div className="bg-primary text-white p-4 rounded-t-lg flex items-center justify-between">
        <div className="flex items-center gap-2">
          <MessageCircle className="w-5 h-5" />
          <h3 className="font-semibold">Assistente IA</h3>
        </div>
        <div className="flex gap-2">
          <Button
            variant="ghost"
            size="icon"
            className="text-white hover:bg-white/20 h-8 w-8"
            onClick={() => setIsMaximized(!isMaximized)}
          >
            {isMaximized ? (
              <Minimize2 className="w-4 h-4" />
            ) : (
              <Maximize2 className="w-4 h-4" />
            )}
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="text-white hover:bg-white/20 h-8 w-8"
            onClick={() => setIsOpen(false)}
          >
            <X className="w-4 h-4" />
          </Button>
        </div>
      </div>

      {/* Messages Area */}
      <ScrollArea className="flex-1 p-4" ref={scrollRef}>
        <div className="space-y-4">
          {messages.map((message, index) => (
            <div
              key={index}
              className={`flex ${
                message.role === "user" ? "justify-end" : "justify-start"
              }`}
            >
              <div
                className={`max-w-[80%] rounded-lg p-3 ${
                  message.role === "user"
                    ? "bg-primary text-white"
                    : "bg-muted text-foreground"
                }`}
              >
                <p className="text-sm whitespace-pre-wrap">{message.content}</p>
              </div>
            </div>
          ))}
        </div>
      </ScrollArea>

      {/* Input Area */}
      <div className="p-4 border-t">
        <div className="flex gap-2">
          <Input
            placeholder="Digite sua pergunta..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            className="flex-1"
          />
          <Button
            onClick={handleSend}
            className="bg-primary hover:bg-primary/90"
            size="icon"
          >
            <Send className="w-4 h-4" />
          </Button>
        </div>
      </div>
    </Card>
  );
};
