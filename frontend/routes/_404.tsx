import { Layout } from "../components/Layout.tsx";
import { Button } from "../components/Button.tsx";

export default function Error404() {
  return (
    <Layout>
      <div className="max-w-md mx-auto py-16 text-center">
        <h1 className="text-6xl font-bold text-gray-800 mb-4">404</h1>
        <p className="text-xl text-gray-600 mb-8">页面未找到</p>
        <p className="text-gray-500 mb-8">您访问的页面不存在或已被移除。</p>
        <Button
          variant="primary"
          onClick={() => globalThis.location.href = "/"}
        >
          返回首页
        </Button>
      </div>
    </Layout>
  );
}
