import { useSignal } from "@preact/signals";
import { Button } from "../components/Button.tsx";
import { Card } from "../components/Card.tsx";
import { Alert } from "../components/Alert.tsx";
import { apiClient } from "../utils/api-client.ts";
import type {
  AnalysisFormFilter,
  AnalysisFormOperation,
  AnalysisOption,
  AnalysisResult,
  ColumnInfo,
  Schema,
} from "../types/analysis.ts";

interface AnalysisFormProps {
  taskId: string;
  onAnalysisComplete?: (result: AnalysisResult) => void;
}

export default function AnalysisForm(
  { taskId, onAnalysisComplete }: AnalysisFormProps,
) {
  const isLoading = useSignal(false);
  const error = useSignal<string | null>(null);
  const schema = useSignal<Schema | null>(null);
  const analysisOptions = useSignal<AnalysisOption[]>([]);
  const operations = useSignal<AnalysisFormOperation[]>([]);
  const filters = useSignal<AnalysisFormFilter[]>([]);
  const isInitialized = useSignal(false);

  console.log("[AnalysisForm] 组件初始化，taskId:", taskId);

  const loadSchemaAndOptions = async () => {
    if (isInitialized.value) {
      console.log("[AnalysisForm] 已经初始化，跳过重复加载");
      return;
    }

    try {
      console.log("[AnalysisForm] 开始加载schema和分析选项");

      schema.value = await apiClient.getSchema(taskId);
      console.log("[AnalysisForm] Schema加载成功:", schema.value);

      const optionsData = await apiClient.getAnalysisOptions(taskId);

      const convertedOptions = optionsData.options.map((
        option: { [key: string]: unknown },
      ) => ({
        name: option.column as string,
        description: `${option.column as string} (${
          (option.operations as string[]).join(", ")
        })`,
        type: "analysis",
        fields: option.operations as string[],
      }));

      analysisOptions.value = convertedOptions;
      console.log("[AnalysisForm] 分析选项加载成功:", analysisOptions.value);

      isInitialized.value = true;
    } catch (err) {
      error.value = err instanceof Error ? err.message : "加载数据时发生错误";
      console.error("[AnalysisForm] 加载数据失败:", err);
    }
  };

  if (typeof window !== "undefined" && !isInitialized.value) {
    setTimeout(() => {
      loadSchemaAndOptions();
    }, 0);
  }

  const addOperation = () => {
    operations.value = [...operations.value, {
      type: "",
      field: "",
      alias: "",
    }];
  };

  const removeOperation = (index: number) => {
    operations.value = operations.value.filter((_, i) => i !== index);
  };

  const updateOperation = (index: number, field: string, value: string) => {
    const updatedOperations = [...operations.value];
    updatedOperations[index] = { ...updatedOperations[index], [field]: value };
    operations.value = updatedOperations;
  };

  const addFilter = () => {
    filters.value = [...filters.value, {
      field: "",
      operator: "=",
      value: "",
    }];
  };

  const removeFilter = (index: number) => {
    filters.value = filters.value.filter((_, i) => i !== index);
  };

  const updateFilter = (index: number, field: string, value: string) => {
    const updatedFilters = [...filters.value];
    updatedFilters[index] = { ...updatedFilters[index], [field]: value };
    filters.value = updatedFilters;
  };

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    console.log("[AnalysisForm] 提交分析请求");

    if (operations.value.length === 0) {
      error.value = "请至少添加一个分析操作";
      console.log("[AnalysisForm] 没有分析操作");
      return;
    }

    try {
      isLoading.value = true;
      error.value = null;

      const convertedOperations = operations.value.map((op) => ({
        column: op.field,
        operation: op.type,
      }));

      const convertedFilters = filters.value.length > 0
        ? filters.value.map((f) => ({
          column: f.field,
          operator: f.operator,
          value: f.value,
        }))
        : null;

      const payload = {
        task_id: taskId,
        operations: convertedOperations,
        filters: convertedFilters,
      };

      console.log("[AnalysisForm] 发送分析请求:", payload);

      const result = await apiClient.executeAnalysis(payload);
      console.log("[AnalysisForm] 分析成功:", result);

      if (onAnalysisComplete) {
        onAnalysisComplete(result);
      }
    } catch (err) {
      console.error("[AnalysisForm] 分析过程中发生错误:", err);
      error.value = err instanceof Error ? err.message : "分析过程中发生错误";
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
        <Card title="分析操作">
          <div className="space-y-4">
            {operations.value.map((operation, index) => (
              <div
                key={index}
                className="flex gap-4 items-end p-4 border rounded-md"
              >
                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    操作类型
                  </label>
                  <select
                    className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    value={operation.type}
                    onChange={(e) =>
                      updateOperation(
                        index,
                        "type",
                        (e.target as HTMLSelectElement).value,
                      )}
                  >
                    <option value="">选择操作类型</option>
                    <option value="SUM">SUM</option>
                    <option value="AVG">AVG</option>
                    <option value="MAX">MAX</option>
                    <option value="MIN">MIN</option>
                    <option value="COUNT">COUNT</option>
                    <option value="COUNT_DISTINCT">COUNT_DISTINCT</option>
                  </select>
                </div>

                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    字段
                  </label>
                  <select
                    className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    value={operation.field}
                    onChange={(e) =>
                      updateOperation(
                        index,
                        "field",
                        (e.target as HTMLSelectElement).value,
                      )}
                  >
                    <option value="">选择字段</option>
                    {schema.value?.columns?.map((column: ColumnInfo) => (
                      <option key={column.name} value={column.name}>
                        {column.name}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    别名
                  </label>
                  <input
                    type="text"
                    className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    value={operation.alias}
                    onChange={(e) =>
                      updateOperation(
                        index,
                        "alias",
                        (e.target as HTMLInputElement).value,
                      )}
                    placeholder="结果列名"
                  />
                </div>

                <Button
                  type="button"
                  variant="danger"
                  onClick={() => removeOperation(index)}
                >
                  删除
                </Button>
              </div>
            ))}

            <Button
              type="button"
              variant="secondary"
              onClick={addOperation}
            >
              添加操作
            </Button>
          </div>
        </Card>

        <Card title="过滤条件（可选）">
          <div className="space-y-4">
            {filters.value.map((filter, index) => (
              <div
                key={index}
                className="flex gap-4 items-end p-4 border rounded-md"
              >
                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    字段
                  </label>
                  <select
                    className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    value={filter.field}
                    onChange={(e) =>
                      updateFilter(
                        index,
                        "field",
                        (e.target as HTMLSelectElement).value,
                      )}
                  >
                    <option value="">选择字段</option>
                    {schema.value?.columns?.map((column: ColumnInfo) => (
                      <option key={column.name} value={column.name}>
                        {column.name}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    操作符
                  </label>
                  <select
                    className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    value={filter.operator}
                    onChange={(e) =>
                      updateFilter(
                        index,
                        "operator",
                        (e.target as HTMLSelectElement).value,
                      )}
                  >
                    <option value="=">=</option>
                    <option value="!=">!=</option>
                    <option value=">">&gt;</option>
                    <option value="<">&lt;</option>
                    <option value=">=">&gt;=</option>
                    <option value="<=">&lt;=</option>
                    <option value="LIKE">LIKE</option>
                  </select>
                </div>

                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    值
                  </label>
                  <input
                    type="text"
                    className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    value={filter.value}
                    onChange={(e) =>
                      updateFilter(
                        index,
                        "value",
                        (e.target as HTMLInputElement).value,
                      )}
                    placeholder="过滤值"
                  />
                </div>

                <Button
                  type="button"
                  variant="danger"
                  onClick={() => removeFilter(index)}
                >
                  删除
                </Button>
              </div>
            ))}

            <Button
              type="button"
              variant="secondary"
              onClick={addFilter}
            >
              添加过滤条件
            </Button>
          </div>
        </Card>

        <div className="pt-4">
          <Button
            type="submit"
            variant="primary"
            isLoading={isLoading.value}
            disabled={isLoading.value || operations.value.length === 0}
          >
            执行分析
          </Button>
        </div>
      </form>
    </div>
  );
}
