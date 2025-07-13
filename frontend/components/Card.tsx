import { JSX } from "preact";

interface CardProps {
  children: JSX.Element | JSX.Element[];
  title?: string;
  className?: string;
}

export function Card({ children, title, className = "" }: CardProps) {
  return (
    <div
      className={`bg-white rounded-lg shadow-md overflow-hidden ${className}`}
    >
      {title && (
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-medium text-gray-800">{title}</h3>
        </div>
      )}
      <div className="p-6">
        {children}
      </div>
    </div>
  );
}
