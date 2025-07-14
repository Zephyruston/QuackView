import { useSignal } from "@preact/signals";
import { Button } from "../components/Button.tsx";
import { Alert } from "../components/Alert.tsx";
import { apiClient } from "../utils/api-client.ts";
import { analysis as analysisLogger } from "../utils/logger.ts";
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

type FieldType = "numeric" | "text" | "date" | "boolean";

type OperationType =
  | "SUM"
  | "AVG"
  | "MAX"
  | "MIN"
  | "COUNT"
  | "COUNT_DISTINCT"
  | "VAR_POP"
  | "STDDEV_POP"
  | "MEDIAN"
  | "QUARTILES"
  | "PERCENTILES"
  | "TOP_K"
  | "VALUE_DISTRIBUTION"
  | "LENGTH_ANALYSIS"
  | "PATTERN_ANALYSIS"
  | "DATE_RANGE"
  | "YEAR_ANALYSIS"
  | "MONTH_ANALYSIS"
  | "DAY_ANALYSIS"
  | "HOUR_ANALYSIS"
  | "WEEKDAY_ANALYSIS"
  | "SEASONAL_ANALYSIS"
  | "MISSING_VALUES"
  | "DATA_QUALITY"
  | "CORRELATION"
  | "SELECT";

type GroupByField = string;

const DUCKDB_NUMERIC_TYPES = new Set([
  "bigint",
  "dec",
  "decimal",
  "double",
  "float",
  "float4",
  "float8",
  "hugeint",
  "int",
  "int1",
  "int128",
  "int16",
  "int2",
  "int32",
  "int4",
  "int64",
  "int8",
  "integer",
  "integral",
  "long",
  "numeric",
  "real",
  "short",
  "signed",
  "smallint",
  "tinyint",
  "ubigint",
  "uhugeint",
  "uint128",
  "uint16",
  "uint32",
  "uint64",
  "uint8",
  "uinteger",
  "usmallint",
  "utinyint",
  "varint",
]);

const _DUCKDB_TEXT_TYPES = new Set([
  "bpchar",
  "char",
  "nvarchar",
  "string",
  "text",
  "varchar",
]);

const DUCKDB_TIME_TYPES = new Set([
  "date",
  "datetime",
  "time",
  "timestamp",
  "timestamp_ms",
  "timestamp_ns",
  "timestamp_s",
  "timestamp_us",
  "timestamptz",
  "timetz",
  "interval",
]);

const OPERATION_TYPE_MAP: Record<FieldType, OperationType[]> = {
  numeric: [
    "SUM",
    "AVG",
    "MAX",
    "MIN",
    "COUNT",
    "COUNT_DISTINCT",
    "VAR_POP",
    "STDDEV_POP",
    "MEDIAN",
    "QUARTILES",
    "PERCENTILES",
    "MISSING_VALUES",
    "DATA_QUALITY",
    "SELECT",
  ],
  text: [
    "COUNT",
    "COUNT_DISTINCT",
    "TOP_K",
    "VALUE_DISTRIBUTION",
    "LENGTH_ANALYSIS",
    "PATTERN_ANALYSIS",
    "MISSING_VALUES",
    "DATA_QUALITY",
    "SELECT",
  ],
  date: [
    "COUNT",
    "COUNT_DISTINCT",
    "DATE_RANGE",
    "YEAR_ANALYSIS",
    "MONTH_ANALYSIS",
    "DAY_ANALYSIS",
    "HOUR_ANALYSIS",
    "WEEKDAY_ANALYSIS",
    "SEASONAL_ANALYSIS",
    "MISSING_VALUES",
    "DATA_QUALITY",
    "SELECT",
  ],
  boolean: [
    "COUNT",
    "COUNT_DISTINCT",
    "MISSING_VALUES",
    "DATA_QUALITY",
    "SELECT",
  ],
};

const OPERATION_DISPLAY_NAMES: Record<OperationType, string> = {
  SUM: "求和",
  AVG: "平均值",
  MAX: "最大值",
  MIN: "最小值",
  COUNT: "计数",
  COUNT_DISTINCT: "唯一值计数",
  VAR_POP: "总体方差",
  STDDEV_POP: "总体标准差",
  MEDIAN: "中位数",
  QUARTILES: "四分位数",
  PERCENTILES: "百分位数",
  TOP_K: "前K高频值",
  VALUE_DISTRIBUTION: "值分布",
  LENGTH_ANALYSIS: "长度分析",
  PATTERN_ANALYSIS: "模式分析",
  DATE_RANGE: "日期范围",
  YEAR_ANALYSIS: "年度分析",
  MONTH_ANALYSIS: "月度分析",
  DAY_ANALYSIS: "日分析",
  HOUR_ANALYSIS: "小时分析",
  WEEKDAY_ANALYSIS: "星期分析",
  SEASONAL_ANALYSIS: "季节性分析",
  MISSING_VALUES: "缺失值分析",
  DATA_QUALITY: "数据质量检查",
  CORRELATION: "相关性分析",
  SELECT: "选择字段",
};

function inferFieldType(column: ColumnInfo): FieldType {
  const type = column.type.toLowerCase();

  if (DUCKDB_NUMERIC_TYPES.has(type)) {
    return "numeric";
  } else if (DUCKDB_TIME_TYPES.has(type)) {
    return "date";
  } else if (type.includes("bool")) {
    return "boolean";
  } else {
    return "text";
  }
}

function getAvailableOperations(column: ColumnInfo): OperationType[] {
  const fieldType = inferFieldType(column);
  return OPERATION_TYPE_MAP[fieldType];
}

type AnalysisRequestWithLimit = {
  task_id: string;
  operations: { column: string; operation: string }[];
  filters?: {
    column: string;
    operator: string;
    value: string | number | unknown[];
  }[] | null;
  group_by?: string[];
  sort_by?: { field: string; order: "ASC" | "DESC" }[];
  limit?: number;
};

export default function AnalysisForm(
  { taskId, onAnalysisComplete }: AnalysisFormProps,
) {
  const isLoading = useSignal(false);
  const error = useSignal<string | null>(null);
  const schema = useSignal<Schema | null>(null);
  const analysisOptions = useSignal<AnalysisOption[]>([]);
  const operations = useSignal<AnalysisFormOperation[]>([]);
  const filters = useSignal<AnalysisFormFilter[]>([]);
  const groupByFields = useSignal<GroupByField[]>([]);
  const sortByFields = useSignal<{ field: string; order: "ASC" | "DESC" }[]>(
    [],
  );
  const isInitialized = useSignal(false);
  const selectedField = useSignal<string>("");
  const limit = useSignal<number | null>(null);

  analysisLogger.debug("分析表单组件初始化，taskId:", taskId);

  const loadSchemaAndOptions = async () => {
    if (isInitialized.value) {
      analysisLogger.debug("已经初始化，跳过重复加载");
      return;
    }

    try {
      analysisLogger.debug("开始加载schema和分析选项");

      schema.value = await apiClient.getSchema(taskId);
      analysisLogger.debug("Schema加载成功:", schema.value);

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
      analysisLogger.debug("分析选项加载成功:", analysisOptions.value);

      isInitialized.value = true;
    } catch (err) {
      error.value = err instanceof Error ? err.message : "加载数据时发生错误";
      analysisLogger.error("加载数据失败:", err);
    }
  };

  if (
    typeof globalThis !== "undefined" && "document" in globalThis &&
    !isInitialized.value
  ) {
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

  const addGroupByField = () => {
    groupByFields.value = [...groupByFields.value, ""];
  };

  const removeGroupByField = (index: number) => {
    groupByFields.value = groupByFields.value.filter((_, i) => i !== index);
  };

  const updateGroupByField = (index: number, value: string) => {
    const updatedFields = [...groupByFields.value];
    updatedFields[index] = value;
    groupByFields.value = updatedFields;
  };

  const addSortByField = () => {
    sortByFields.value = [...sortByFields.value, { field: "", order: "ASC" }];
  };
  const removeSortByField = (index: number) => {
    sortByFields.value = sortByFields.value.filter((_, i) => i !== index);
  };
  const updateSortByField = (index: number, field: string, value: string) => {
    const updated = [...sortByFields.value];
    updated[index] = { ...updated[index], [field]: value };
    sortByFields.value = updated;
  };

  const handleFieldChange = (index: number, fieldValue: string) => {
    updateOperation(index, "field", fieldValue);
    updateOperation(index, "type", "");
    selectedField.value = fieldValue;
  };

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    analysisLogger.debug("提交分析请求");

    if (operations.value.length === 0) {
      error.value = "请至少添加一个分析操作";
      analysisLogger.debug("没有分析操作");
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

      let convertedGroupBy = groupByFields.value.length > 0
        ? groupByFields.value.filter((field) => field.trim() !== "")
        : undefined;

      if (
        operations.value.length === 1 &&
        operations.value[0].type === "SELECT" &&
        (!groupByFields.value || groupByFields.value.length === 0)
      ) {
        convertedGroupBy = undefined;
      }

      const convertedSortBy = sortByFields.value.length > 0
        ? sortByFields.value.filter((s) => s.field).map((s) => ({
          field: s.field,
          order: s.order,
        }))
        : undefined;

      const payload: AnalysisRequestWithLimit = {
        task_id: taskId,
        operations: convertedOperations,
        filters: convertedFilters,
        group_by: convertedGroupBy,
        sort_by: convertedSortBy,
      };
      if (
        operations.value.length === 1 &&
        operations.value[0].type === "SELECT" &&
        limit.value && limit.value > 0
      ) {
        payload.limit = limit.value;
      }

      analysisLogger.debug("发送分析请求:", payload);

      const result = await apiClient.executeAnalysis(payload);
      analysisLogger.info("分析成功:", result);

      if (onAnalysisComplete) {
        onAnalysisComplete(result);
      }
    } catch (err) {
      analysisLogger.error("分析过程中发生错误:", err);
      error.value = err instanceof Error ? err.message : "分析过程中发生错误";
    } finally {
      isLoading.value = false;
    }
  };

  const getFieldType = (fieldName: string): FieldType => {
    const column = schema.value?.columns?.find((col) => col.name === fieldName);
    return column ? inferFieldType(column) : "text";
  };

  const getFieldTypeLabel = (fieldType: FieldType): string => {
    const labels = {
      numeric: "数值",
      text: "文本",
      date: "日期",
      boolean: "布尔",
    };
    return labels[fieldType];
  };

  return (
    <div className="space-y-4">
      {error.value && (
        <Alert
          type="error"
          message={error.value}
          className="mb-3"
          onClose={() => error.value = null}
        />
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="bg-white border border-gray-200 rounded-md p-3 shadow-sm">
          <div className="flex items-center mb-2">
            <h3 className="text-base font-medium text-gray-900 mr-2">
              分析操作
            </h3>
            <div className="relative group">
              <svg
                className="w-5 h-5 text-gray-400 cursor-help"
                fill="currentColor"
                viewBox="0 0 20 20"
              >
                <path
                  fillRule="evenodd"
                  d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z"
                  clipRule="evenodd"
                />
              </svg>
              <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-sm rounded-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                选择字段的计算逻辑（求和/计数等）
                <div className="absolute top-full left-1/2 transform -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-gray-900">
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-2">
            {operations.value.map((operation, index) => {
              const fieldType = getFieldType(operation.field);
              const availableOperations = operation.field
                ? getAvailableOperations(
                  schema.value?.columns?.find((col) =>
                    col.name === operation.field
                  ) || { name: "", type: "" },
                )
                : [];

              return (
                <div
                  key={index}
                  className="bg-gray-50 border border-gray-200 rounded-md p-3 space-y-2 shadow-sm"
                >
                  <div className="flex items-center justify-between">
                    <h4 className="text-base font-medium text-gray-900">
                      操作 #{index + 1}
                    </h4>
                    <Button
                      type="button"
                      variant="danger"
                      size="sm"
                      onClick={() => removeOperation(index)}
                      className="text-red-600 bg-red-50 hover:bg-red-100 border-red-200 h-7 px-2 text-xs"
                    >
                      删除
                    </Button>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                    <div>
                      <label className="block text-xs font-medium text-gray-700 mb-1">
                        字段
                      </label>
                      <select
                        className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 transition-colors text-base h-8"
                        value={operation.field}
                        onChange={(e) =>
                          handleFieldChange(
                            index,
                            (e.target as HTMLSelectElement).value,
                          )}
                      >
                        <option value="" style={{ fontSize: "16px" }}>
                          选择分析字段（例：用户ID、订单金额）
                        </option>
                        {schema.value?.columns?.map((column: ColumnInfo) => (
                          <option
                            key={column.name}
                            value={column.name}
                            style={{ fontSize: "16px" }}
                          >
                            {column.name}{" "}
                            ({getFieldTypeLabel(inferFieldType(column))})
                          </option>
                        ))}
                      </select>
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-gray-700 mb-1">
                        操作类型
                      </label>
                      <select
                        className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 transition-colors text-base disabled:bg-gray-100 disabled:text-gray-500 h-8"
                        value={operation.type}
                        onChange={(e) =>
                          updateOperation(
                            index,
                            "type",
                            (e.target as HTMLSelectElement).value,
                          )}
                        disabled={!operation.field}
                      >
                        <option value="" style={{ fontSize: "16px" }}>
                          {operation.field ? "选择操作类型" : "请先选择字段"}
                        </option>
                        {availableOperations.map((op) => (
                          <option
                            key={op}
                            value={op}
                            style={{ fontSize: "16px" }}
                          >
                            {OPERATION_DISPLAY_NAMES[op] || op}
                          </option>
                        ))}
                      </select>
                      {operation.field && (
                        <p className="text-xs text-gray-500 mt-1">
                          字段类型: {getFieldTypeLabel(fieldType)}
                        </p>
                      )}
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-gray-700 mb-1">
                        别名
                        <span className="relative group ml-1">
                          <svg
                            className="w-4 h-4 text-gray-400 cursor-help inline"
                            fill="currentColor"
                            viewBox="0 0 20 20"
                          >
                            <path
                              fillRule="evenodd"
                              d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z"
                              clipRule="evenodd"
                            />
                          </svg>
                          <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-sm rounded-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10 w-64">
                            别名用于自定义结果列名，可不填（系统自动生成）
                            <div className="absolute top-full left-1/2 transform -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-gray-900">
                            </div>
                          </div>
                        </span>
                      </label>
                      <input
                        type="text"
                        className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 transition-colors text-base h-8"
                        value={operation.alias}
                        onChange={(e) =>
                          updateOperation(
                            index,
                            "alias",
                            (e.target as HTMLInputElement).value,
                          )}
                        placeholder="结果列名（可选）"
                        style={{ maxWidth: "140px" }}
                      />
                    </div>
                  </div>
                </div>
              );
            })}
            {operations.value.length === 1 &&
              operations.value[0].type === "SELECT" && (
              <div className="flex items-center gap-2 mt-2">
                <label className="text-sm text-gray-700">
                  限制条数(limit):
                </label>
                <input
                  type="number"
                  min={1}
                  className="w-32 rounded-md border border-gray-300 px-2 py-1 text-base focus:border-blue-500 focus:ring-blue-500"
                  value={limit.value ?? ""}
                  onInput={(e) => {
                    const v = (e.target as HTMLInputElement).value;
                    limit.value = v ? parseInt(v, 10) : null;
                  }}
                  placeholder="如 100"
                />
              </div>
            )}

            <Button
              type="button"
              variant="secondary"
              onClick={addOperation}
              className="w-full py-2 border-2 border-dashed border-gray-300 hover:border-blue-500 hover:bg-blue-50 transition-all duration-200 text-gray-600 font-medium text-sm"
            >
              <svg
                className="w-4 h-4 mr-2"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 6v6m0 0v6m0-6h6m-6 0H6"
                />
              </svg>
              添加分析操作
            </Button>
          </div>
        </div>

        <div className="bg-white border border-gray-200 rounded-md p-3 shadow-sm">
          <div className="flex items-center mb-2">
            <h3 className="text-base font-medium text-gray-900 mr-2">
              聚合条件
            </h3>
            <div className="relative group">
              <svg
                className="w-5 h-5 text-gray-400 cursor-help"
                fill="currentColor"
                viewBox="0 0 20 20"
              >
                <path
                  fillRule="evenodd"
                  d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z"
                  clipRule="evenodd"
                />
              </svg>
              <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-sm rounded-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                按字段分组（如日期/地区）
                <div className="absolute top-full left-1/2 transform -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-gray-900">
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-2">
            {groupByFields.value.map((field, index) => (
              <div
                key={index}
                className="bg-gray-50 border border-gray-200 rounded-md p-3 space-y-2 shadow-sm"
              >
                <div className="flex items-center justify-between">
                  <h4 className="text-base font-medium text-gray-900">
                    分组字段 #{index + 1}
                  </h4>
                  <Button
                    type="button"
                    variant="danger"
                    size="sm"
                    onClick={() => removeGroupByField(index)}
                    className="text-red-600 bg-red-50 hover:bg-red-100 border-red-200 h-7 px-2 text-xs"
                  >
                    删除
                  </Button>
                </div>

                <div>
                  <label className="block text-xs font-medium text-gray-700 mb-1">
                    分组字段
                  </label>
                  <select
                    className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 transition-colors text-base h-8"
                    value={field}
                    onChange={(e) =>
                      updateGroupByField(
                        index,
                        (e.target as HTMLSelectElement).value,
                      )}
                  >
                    <option value="" style={{ fontSize: "16px" }}>
                      选择分组字段
                    </option>
                    {schema.value?.columns?.map((column: ColumnInfo) => (
                      <option
                        key={column.name}
                        value={column.name}
                        style={{ fontSize: "16px" }}
                      >
                        {column.name}{" "}
                        ({getFieldTypeLabel(inferFieldType(column))})
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            ))}

            <Button
              type="button"
              variant="secondary"
              onClick={addGroupByField}
              className="w-full py-2 border-2 border-dashed border-gray-300 hover:border-blue-500 hover:bg-blue-50 transition-all duration-200 text-gray-600 font-medium text-sm"
            >
              <svg
                className="w-4 h-4 mr-2"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 6v6m0 0v6m0-6h6m-6 0H6"
                />
              </svg>
              添加分组字段
            </Button>
          </div>
        </div>

        <div className="bg-white border border-gray-200 rounded-md p-3 shadow-sm">
          <div className="flex items-center mb-2">
            <h3 className="text-base font-medium text-gray-900 mr-2">
              过滤条件
            </h3>
            <div className="relative group">
              <svg
                className="w-5 h-5 text-gray-400 cursor-help"
                fill="currentColor"
                viewBox="0 0 20 20"
              >
                <path
                  fillRule="evenodd"
                  d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z"
                  clipRule="evenodd"
                />
              </svg>
              <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-sm rounded-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                设置数据过滤条件
                <div className="absolute top-full left-1/2 transform -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-gray-900">
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-2">
            {filters.value.map((filter, index) => (
              <div
                key={index}
                className="bg-gray-50 border border-gray-200 rounded-md p-3 space-y-2 shadow-sm"
              >
                <div className="flex items-center justify-between">
                  <h4 className="text-base font-medium text-gray-900">
                    过滤条件 #{index + 1}
                  </h4>
                  <Button
                    type="button"
                    variant="danger"
                    size="sm"
                    onClick={() => removeFilter(index)}
                    className="text-red-600 bg-red-50 hover:bg-red-100 border-red-200 h-7 px-2 text-xs"
                  >
                    删除
                  </Button>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">
                      字段
                    </label>
                    <select
                      className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 transition-colors text-base h-8"
                      value={filter.field}
                      onChange={(e) =>
                        updateFilter(
                          index,
                          "field",
                          (e.target as HTMLSelectElement).value,
                        )}
                    >
                      <option value="" style={{ fontSize: "16px" }}>
                        选择字段
                      </option>
                      {schema.value?.columns?.map((column: ColumnInfo) => (
                        <option
                          key={column.name}
                          value={column.name}
                          style={{ fontSize: "16px" }}
                        >
                          {column.name}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">
                      操作符
                    </label>
                    <select
                      className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 transition-colors text-base h-8"
                      value={filter.operator}
                      onChange={(e) =>
                        updateFilter(
                          index,
                          "operator",
                          (e.target as HTMLSelectElement).value,
                        )}
                    >
                      <option value="=" style={{ fontSize: "16px" }}>=</option>
                      <option value="!=" style={{ fontSize: "16px" }}>
                        !=
                      </option>
                      <option value=">" style={{ fontSize: "16px" }}>
                        &gt;
                      </option>
                      <option value="<" style={{ fontSize: "16px" }}>
                        &lt;
                      </option>
                      <option value=">=" style={{ fontSize: "16px" }}>
                        &gt;=
                      </option>
                      <option value="<=" style={{ fontSize: "16px" }}>
                        &lt;=
                      </option>
                      <option value="LIKE" style={{ fontSize: "16px" }}>
                        LIKE
                      </option>
                    </select>
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">
                      值
                    </label>
                    <input
                      type="text"
                      className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 transition-colors text-xs h-8"
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
                </div>
              </div>
            ))}

            <Button
              type="button"
              variant="secondary"
              onClick={addFilter}
              className="w-full py-2 border-2 border-dashed border-gray-300 hover:border-blue-500 hover:bg-blue-50 transition-all duration-200 text-gray-600 font-medium text-sm"
            >
              <svg
                className="w-4 h-4 mr-2"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"
                />
              </svg>
              添加过滤条件
            </Button>
          </div>
        </div>

        <div className="bg-white border border-gray-200 rounded-md p-3 shadow-sm">
          <div className="flex items-center mb-2">
            <h3 className="text-base font-medium text-gray-900 mr-2">
              排序条件
            </h3>
            <div className="relative group">
              <svg
                className="w-5 h-5 text-gray-400 cursor-help"
                fill="currentColor"
                viewBox="0 0 20 20"
              >
                <path
                  fillRule="evenodd"
                  d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z"
                  clipRule="evenodd"
                />
              </svg>
              <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-sm rounded-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none whitespace-nowrap z-10">
                可对结果按一个或多个字段排序，支持升序/降序。
                <div className="absolute top-full left-1/2 transform -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-gray-900">
                </div>
              </div>
            </div>
          </div>
          <div className="space-y-2">
            {sortByFields.value.map((sort, index) => (
              <div key={index} className="flex items-center gap-2 mb-1">
                <select
                  className="rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-base h-8"
                  value={sort.field}
                  onChange={(e) =>
                    updateSortByField(
                      index,
                      "field",
                      (e.target as HTMLSelectElement).value,
                    )}
                >
                  <option value="">选择字段</option>
                  {schema.value?.columns?.map((col) => (
                    <option key={col.name} value={col.name}>{col.name}</option>
                  ))}
                </select>
                <select
                  className="rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-base h-8"
                  value={sort.order}
                  onChange={(e) =>
                    updateSortByField(
                      index,
                      "order",
                      (e.target as HTMLSelectElement).value,
                    )}
                >
                  <option value="ASC">升序</option>
                  <option value="DESC">降序</option>
                </select>
                <Button
                  type="button"
                  variant="danger"
                  size="sm"
                  onClick={() =>
                    removeSortByField(index)}
                  className="h-7 px-2 text-xs"
                >
                  删除
                </Button>
              </div>
            ))}
            <Button
              type="button"
              variant="secondary"
              onClick={addSortByField}
              className="w-full py-2 border-2 border-dashed border-gray-300 hover:border-blue-500 hover:bg-blue-50 transition-all duration-200 text-gray-600 font-medium text-sm"
            >
              <svg
                className="w-4 h-4 mr-2"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 6v6m0 0v6m0-6h6m-6 0H6"
                />
              </svg>
              添加排序条件
            </Button>
          </div>
        </div>

        <div className="pt-4">
          <Button
            type="submit"
            variant="primary"
            isLoading={isLoading.value}
            disabled={isLoading.value || operations.value.length === 0}
            className="w-full py-2 text-base font-medium bg-blue-600 hover:bg-blue-700 text-white rounded-md shadow-sm transition-all duration-200 transform hover:scale-[1.01] active:scale-[0.98]"
          >
            {isLoading.value ? "执行分析中..." : "执行分析"}
          </Button>
        </div>
      </form>
    </div>
  );
}
