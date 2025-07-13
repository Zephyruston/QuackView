export function Footer() {
  return (
    <footer className="bg-gray-100 text-gray-600 py-4 mt-auto">
      <div className="max-w-screen-xl mx-auto px-4 text-center">
        <p>© {new Date().getFullYear()} QuackView - Excel数据分析工具</p>
        <p className="text-sm mt-1">
          Click & Analyze - 简单高效的Excel数据分析
        </p>
      </div>
    </footer>
  );
}
