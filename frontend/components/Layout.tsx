import { JSX } from "preact";
import { Header } from "./Header.tsx";
import { Footer } from "./Footer.tsx";

interface LayoutProps {
  children: JSX.Element | JSX.Element[];
  title?: string;
}

export function Layout({ children, title }: LayoutProps) {
  return (
    <div className="flex flex-col min-h-screen">
      <Header />
      <main className="flex-grow py-6">
        {title && (
          <div className="max-w-screen-xl mx-auto px-4 mb-6">
            <h2 className="text-2xl font-bold text-gray-800">{title}</h2>
          </div>
        )}
        <div className="max-w-screen-xl mx-auto px-4">
          {children}
        </div>
      </main>
      <Footer />
    </div>
  );
}
