import { Layout } from "../components/Layout.tsx";
import { Card } from "../components/Card.tsx";

export default function About() {
  return (
    <Layout title="关于 QuackView">
      <div className="max-w-4xl mx-auto">
        <Card className="mb-6">
          <h3 className="text-xl font-bold text-gray-800 mb-4">
            什么是 QuackView?
          </h3>
          <p className="text-gray-600 mb-4">
            QuackView 是一个简单易用的 Excel
            数据分析工具，旨在帮助用户快速分析和可视化 Excel
            数据，无需编程知识。
            通过简单的界面操作，您可以执行复杂的数据分析任务，并以多种格式导出结果。
          </p>
          <p className="text-gray-600">
            QuackView 的名称来源于 "Quick" 和
            "View"，象征着快速查看和分析数据的能力。
          </p>
        </Card>

        <Card className="mb-6">
          <h3 className="text-xl font-bold text-gray-800 mb-4">主要功能</h3>
          <ul className="list-disc list-inside text-gray-600 space-y-2">
            <li>Excel 文件上传与解析</li>
            <li>自动识别数据类型和表结构</li>
            <li>可视化数据分析操作</li>
            <li>自定义 SQL 查询支持</li>
            <li>分析结果导出 (Excel, SQL)</li>
            <li>多种数据过滤和聚合方式</li>
          </ul>
        </Card>

        <Card className="mb-6">
          <h3 className="text-xl font-bold text-gray-800 mb-4">使用方法</h3>
          <ol className="list-decimal list-inside text-gray-600 space-y-4">
            <li>
              <span className="font-medium">上传 Excel 文件</span>
              <p className="ml-6 mt-1">
                在首页上传您的 Excel 文件 (.xlsx 或 .xls 格式)
              </p>
            </li>
            <li>
              <span className="font-medium">选择分析方式</span>
              <p className="ml-6 mt-1">
                使用可视化分析界面或编写自定义 SQL 查询
              </p>
            </li>
            <li>
              <span className="font-medium">配置分析操作</span>
              <p className="ml-6 mt-1">选择要分析的列、操作类型和过滤条件</p>
            </li>
            <li>
              <span className="font-medium">查看分析结果</span>
              <p className="ml-6 mt-1">以表格形式查看分析结果和 SQL 预览</p>
            </li>
            <li>
              <span className="font-medium">导出结果</span>
              <p className="ml-6 mt-1">将结果导出为 Excel 文件或 SQL 脚本</p>
            </li>
          </ol>
        </Card>

        <Card>
          <h3 className="text-xl font-bold text-gray-800 mb-4">技术栈</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <h4 className="font-medium text-gray-700 mb-2">前端</h4>
              <ul className="list-disc list-inside text-gray-600 space-y-1">
                <li>Deno + Fresh 框架</li>
                <li>Preact 组件库</li>
                <li>Tailwind CSS 样式</li>
                <li>TypeScript 类型系统</li>
              </ul>
            </div>
            <div>
              <h4 className="font-medium text-gray-700 mb-2">后端</h4>
              <ul className="list-disc list-inside text-gray-600 space-y-1">
                <li>Python FastAPI 框架</li>
                <li>Duckdb 内存数据库</li>
                <li>Pandas 数据处理</li>
                <li>OpenAPI 规范接口</li>
              </ul>
            </div>
          </div>
        </Card>
      </div>
    </Layout>
  );
}
