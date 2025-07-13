import { Layout } from "../components/Layout.tsx";
import { Alert } from "../components/Alert.tsx";
import { Button } from "../components/Button.tsx";
import AnalysisContainer from "../islands/AnalysisContainer.tsx";

export default function Analyze(props: { url: URL }) {
  const taskId = props.url.searchParams.get("task_id");

  console.log("[Analyze] 组件初始化，taskId:", taskId);

  if (!taskId) {
    return (
      <Layout>
        <div className="max-w-3xl mx-auto py-8">
          <Alert
            type="error"
            message="缺少任务ID参数，请返回首页重新上传文件"
            className="mb-4"
          />
          <div className="flex justify-center mt-4">
            <Button
              variant="primary"
              onClick={() => globalThis.location.href = "/"}
            >
              返回首页
            </Button>
          </div>
        </div>
      </Layout>
    );
  }

  return <AnalysisContainer taskId={taskId} />;
}
