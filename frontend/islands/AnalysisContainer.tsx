import { useSignal } from "@preact/signals";
import { Layout } from "../components/Layout.tsx";
import { Button } from "../components/Button.tsx";
import AnalysisForm from "./AnalysisForm.tsx";
import AnalysisResult from "./AnalysisResult.tsx";
import CustomQuery from "./CustomQuery.tsx";
import { apiClient } from "../utils/api-client.ts";
import type { AnalysisResult as AnalysisResultType } from "../types/analysis.ts";

interface AnalysisContainerProps {
  taskId: string;
}

export default function AnalysisContainer({ taskId }: AnalysisContainerProps) {
  const analysisResult = useSignal<AnalysisResultType | null>(null);
  const activeTab = useSignal<"form" | "custom">("form");

  console.log("[AnalysisContainer] 组件初始化，taskId:", taskId);

  const resetAnalysis = () => {
    console.log("[AnalysisContainer] 重置分析结果");
    analysisResult.value = null;
  };

  const handleAnalysisComplete = (result: AnalysisResultType) => {
    console.log("[AnalysisContainer] 收到分析结果:", result);
    analysisResult.value = result;
    globalThis.scrollTo({ top: 0, behavior: "smooth" });
  };

  const closeConnection = async () => {
    if (!taskId) return;

    try {
      await apiClient.closeConnection(taskId);
      globalThis.location.href = "/";
    } catch (error) {
      console.error("关闭会话失败:", error);
    }
  };

  console.log(
    "[AnalysisContainer] 渲染组件，activeTab:",
    activeTab.value,
    "analysisResult:",
    !!analysisResult.value,
  );

  return (
    <Layout title="数据分析">
      <div className="mb-6 flex justify-between items-center">
        <div className="flex space-x-4">
          <Button
            variant={activeTab.value === "form" ? "primary" : "secondary"}
            onClick={() => activeTab.value = "form"}
            disabled={!!analysisResult.value}
          >
            可视化分析
          </Button>
          <Button
            variant={activeTab.value === "custom" ? "primary" : "secondary"}
            onClick={() => activeTab.value = "custom"}
            disabled={!!analysisResult.value}
          >
            自定义SQL
          </Button>
        </div>

        <Button
          variant="danger"
          onClick={closeConnection}
        >
          关闭会话
        </Button>
      </div>

      {analysisResult.value
        ? (
          <AnalysisResult
            result={analysisResult.value}
            taskId={taskId}
            onReset={resetAnalysis}
          />
        )
        : (
          <div>
            {activeTab.value === "form" && (
              <AnalysisForm
                taskId={taskId}
                onAnalysisComplete={handleAnalysisComplete}
              />
            )}

            {activeTab.value === "custom" && (
              <CustomQuery
                taskId={taskId}
                onQueryComplete={handleAnalysisComplete}
              />
            )}
          </div>
        )}
    </Layout>
  );
}
