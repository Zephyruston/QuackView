import { useSignal } from "@preact/signals";
import { Button } from "../components/Button.tsx";
import { Card } from "../components/Card.tsx";
import { Alert } from "../components/Alert.tsx";
import { apiClient } from "../utils/api-client.ts";
import type {
  AnalysisResult,
  ColumnInfo,
  Schema,
  SessionInfo,
} from "../types/analysis.ts";

interface CustomQueryProps {
  taskId: string;
  onQueryComplete?: (result: AnalysisResult) => void;
}

export default function CustomQuery(
  { taskId, onQueryComplete }: CustomQueryProps,
) {
  const isLoading = useSignal(false);
  const error = useSignal<string | null>(null);
  const sql = useSignal("");
  const sessionInfo = useSignal<SessionInfo | null>(null);
  const schema = useSignal<Schema | null>(null);
  const isInitialized = useSignal(false);

  console.log("[CustomQuery] 组件初始化，taskId:", taskId);

  const loadSchema = async () => {
    schema.value = await apiClient.getSchema(taskId);
  };

  const loadSessionInfo = async () => {
    if (isInitialized.value) {
      console.log("[CustomQuery] 已经初始化，跳过重复加载");
      return;
    }

    try {
      console.log("[CustomQuery] 开始加载会话信息");
      sessionInfo.value = await apiClient.getSessionInfo(taskId);
      console.log("[CustomQuery] 会话信息加载成功:", sessionInfo.value);
      isInitialized.value = true;
      await loadSchema();
    } catch (err) {
      error.value = err instanceof Error
        ? err.message
        : "加载会话信息时发生错误";
      console.error("[CustomQuery] 加载会话信息失败:", err);
    }
  };

  if (typeof window !== "undefined" && !isInitialized.value) {
    setTimeout(() => {
      loadSessionInfo();
    }, 0);
  }

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    console.log("[CustomQuery] 提交自定义查询");

    if (!sql.value.trim()) {
      error.value = "请输入SQL查询";
      console.log("[CustomQuery] SQL查询为空");
      return;
    }

    try {
      isLoading.value = true;
      error.value = null;

      const payload = {
        task_id: taskId,
        sql: sql.value,
      };

      console.log("[CustomQuery] 发送查询请求:", payload);

      const result = await apiClient.executeCustomQuery(payload);
      console.log("[CustomQuery] 查询成功:", result);

      if (onQueryComplete) {
        onQueryComplete(result);
      }
    } catch (err) {
      console.error("[CustomQuery] 查询过程中发生错误:", err);
      error.value = err instanceof Error ? err.message : "查询过程中发生错误";
    } finally {
      isLoading.value = false;
    }
  };

  return (
    <div>
      {error.value && (
        <Alert
          type="error"
          message={error.value}
          className="mb-4"
          onClose={() => error.value = null}
        />
      )}

      <form onSubmit={handleSubmit} className="space-y-6">
        <Card title="自定义SQL查询">
          <div className="space-y-4">
            {sessionInfo.value && (
              <div className="bg-gray-50 p-4 rounded-md">
                <h4 className="font-medium text-gray-700 mb-2">可用表:</h4>
                {sessionInfo.value.table_name
                  ? (
                    <ul className="list-disc list-inside text-gray-600">
                      <li>{sessionInfo.value.table_name}</li>
                    </ul>
                  )
                  : <span className="text-gray-400">无</span>}
                {schema.value?.columns && (
                  <div className="bg-gray-50 p-4 rounded-md mt-2">
                    <h4 className="font-medium text-gray-700 mb-2">
                      可用字段:
                    </h4>
                    <ul className="list-disc list-inside text-gray-600 flex flex-wrap gap-4">
                      {schema.value.columns.map((col: ColumnInfo) => (
                        <li key={col.name}>
                          {col.name}{" "}
                          <span className="text-xs text-gray-400">
                            ({col.type})
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                SQL查询
              </label>
              <textarea
                className="w-full h-40 rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 font-mono"
                value={sql.value}
                onChange={(e) =>
                  sql.value = (e.target as HTMLTextAreaElement).value}
                placeholder="SELECT * FROM your_table WHERE condition"
              />
            </div>

            <div className="pt-2">
              <Button
                type="submit"
                variant="primary"
                isLoading={isLoading.value}
                disabled={isLoading.value || !sql.value.trim()}
              >
                执行查询
              </Button>
            </div>
          </div>
        </Card>
      </form>
    </div>
  );
}
