import { useSignal } from "@preact/signals";
import { Layout } from "../components/Layout.tsx";
import { Card } from "../components/Card.tsx";
import FileUpload from "../islands/FileUpload.tsx";

export default function Home() {
  const taskId = useSignal<string | null>(null);

  const handleUploadSuccess = (id: string) => {
    console.log("[Home] 文件上传成功，task_id:", id);
    taskId.value = id;

    if (typeof window !== "undefined") {
      location.replace(`/analyze?task_id=${encodeURIComponent(id)}`);
    }
  };

  return (
    <Layout>
      <div className="max-w-3xl mx-auto py-8">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-gray-800 mb-4">
            QuackView - Excel数据分析工具
          </h1>
          <p className="text-lg text-gray-600">
            上传您的Excel文件，快速进行数据分析和可视化
          </p>
        </div>

        <Card>
          <FileUpload onSuccess={handleUploadSuccess} />
        </Card>

        <div className="mt-12 grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="bg-blue-50 p-6 rounded-lg shadow-sm">
            <h3 className="text-lg font-medium text-blue-800 mb-2">简单易用</h3>
            <p className="text-blue-600">
              无需编程知识，通过简单的界面操作即可完成复杂的数据分析任务
            </p>
          </div>

          <div className="bg-green-50 p-6 rounded-lg shadow-sm">
            <h3 className="text-lg font-medium text-green-800 mb-2">
              快速分析
            </h3>
            <p className="text-green-600">
              高效的分析引擎，快速处理大型Excel文件，节省您的宝贵时间
            </p>
          </div>

          <div className="bg-purple-50 p-6 rounded-lg shadow-sm">
            <h3 className="text-lg font-medium text-purple-800 mb-2">
              灵活导出
            </h3>
            <p className="text-purple-600">
              支持导出为Excel或SQL格式，方便与其他系统集成或进一步分析
            </p>
          </div>
        </div>
      </div>
    </Layout>
  );
}
