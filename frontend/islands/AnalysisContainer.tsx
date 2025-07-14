import { useSignal } from "@preact/signals";
import { useEffect } from "preact/hooks";
import { Layout } from "../components/Layout.tsx";
import { Button } from "../components/Button.tsx";
import AnalysisForm from "./AnalysisForm.tsx";
import AnalysisResult from "./AnalysisResult.tsx";
import CustomQuery from "./CustomQuery.tsx";
import { apiClient } from "../utils/api-client.ts";
import { ui as uiLogger } from "../utils/logger.ts";
import type { AnalysisResult as AnalysisResultType } from "../types/analysis.ts";

interface AnalysisContainerProps {
  taskId: string;
}

export default function AnalysisContainer({ taskId }: AnalysisContainerProps) {
  const analysisResult = useSignal<AnalysisResultType | null>(null);
  const activeTab = useSignal<"form" | "custom">("form");

  useEffect(() => {
    apiClient.getSessionInfo(taskId)
      .catch((err) => {
        if (
          err?.message?.includes("not found") ||
          err?.message?.includes("SESSION_NOT_FOUND") ||
          err?.message?.includes("404")
        ) {
          globalThis.location.href = "/";
        }
      });
  }, [taskId]);

  uiLogger.debug("组件初始化，taskId:", taskId);

  const resetAnalysis = () => {
    uiLogger.debug("重置分析结果");
    analysisResult.value = null;
  };

  const handleAnalysisComplete = (result: AnalysisResultType) => {
    uiLogger.info("收到分析结果:", result);
    analysisResult.value = result;
    globalThis.scrollTo({ top: 0, behavior: "smooth" });
  };

  const closeConnection = async () => {
    if (!taskId) return;

    try {
      await apiClient.closeConnection(taskId);
      globalThis.location.href = "/";
    } catch (error) {
      uiLogger.error("关闭会话失败:", error);
    }
  };

  uiLogger.debug(
    "渲染组件，activeTab:",
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
            onClick={() => {
              activeTab.value = "form";
              analysisResult.value = null;
            }}
          >
            可视化分析
          </Button>
          <Button
            variant={activeTab.value === "custom" ? "primary" : "secondary"}
            onClick={() => {
              activeTab.value = "custom";
              analysisResult.value = null;
            }}
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
