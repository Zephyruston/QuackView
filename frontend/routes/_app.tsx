import { type PageProps } from "$fresh/server.ts";
export default function App({ Component }: PageProps) {
  return (
    <html>
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>QuackView - Excel数据分析</title>
        <link rel="stylesheet" href="/styles.css" />
      </head>
      <body className="bg-gray-50 min-h-screen">
        <Component />
      </body>
    </html>
  );
}
