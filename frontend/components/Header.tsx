export function Header() {
  return (
    <header className="bg-blue-600 text-white shadow-md">
      <div className="max-w-screen-xl mx-auto px-4 py-3 flex justify-between items-center">
        <div className="flex items-center space-x-2">
          <img
            src="/logo.svg"
            alt="QuackView Logo"
            className="w-8 h-8"
          />
          <h1 className="text-xl font-bold">QuackView</h1>
        </div>
        <nav>
          <ul className="flex space-x-6">
            <li>
              <a href="/" className="hover:text-blue-200 transition-colors">
                首页
              </a>
            </li>
            <li>
              <a
                href="/about"
                className="hover:text-blue-200 transition-colors"
              >
                关于
              </a>
            </li>
          </ul>
        </nav>
      </div>
    </header>
  );
}
