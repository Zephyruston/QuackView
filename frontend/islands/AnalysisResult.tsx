import { useSignal } from "@preact/signals";
import { Button } from "../components/Button.tsx";
import { Card } from "../components/Card.tsx";
import { Table } from "../components/Table.tsx";
import { Alert } from "../components/Alert.tsx";
import { apiClient } from "../utils/api-client.ts";
import type { AnalysisResultRows } from "../types/analysis.ts";

interface AnalysisResultProps {
  result: {
    columns: string[];
    rows: AnalysisResultRows;
    sql_preview: string;
  };
  taskId: string;
  onReset?: () => void;
}

export default function AnalysisResult(
  { result, taskId, onReset }: AnalysisResultProps,
) {
  const isExporting = useSignal(false);
  const error = useSignal<string | null>(null);
  const success = useSignal<string | null>(null);

  const exportSql = async () => {
    try {
      isExporting.value = true;
      error.value = null;
      success.value = null;

      const blob = await apiClient.exportSql(taskId);
      const url = globalThis.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "analysis.sql";
      document.body.appendChild(a);
      a.click();
      globalThis.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      success.value = "SQL导出成功！";
    } catch (err) {
      error.value = err instanceof Error ? err.message : "导出过程中发生错误";
    } finally {
      isExporting.value = false;
    }
  };

  const exportExcel = async () => {
    try {
      isExporting.value = true;
      error.value = null;
      success.value = null;

      const blob = await apiClient.exportExcel(taskId);
      const url = globalThis.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "analysis_result.xlsx";
      document.body.appendChild(a);
      a.click();
      globalThis.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      success.value = "Excel导出成功！";
    } catch (err) {
      error.value = err instanceof Error ? err.message : "导出过程中发生错误";
    } finally {
      isExporting.value = false;
    }
  };

  return (
    <div className="space-y-6">
      {error.value && (
        <Alert
          type="error"
          message={error.value}
          className="mb-4"
          onClose={() => error.value = null}
        />
      )}

      {success.value && (
        <Alert
          type="success"
          message={success.value}
          className="mb-4"
          onClose={() => success.value = null}
        />
      )}

      <Card title="分析结果">
        <div className="overflow-x-auto">
          <Table
            columns={result.columns}
            rows={result.rows}
            emptyMessage="分析结果为空"
          />
        </div>

        <div className="mt-6 flex flex-wrap gap-4 justify-center">
          <Button
            variant="primary"
            onClick={exportExcel}
            isLoading={isExporting.value}
            disabled={isExporting.value}
          >
            导出Excel
          </Button>

          <Button
            variant="secondary"
            onClick={exportSql}
            isLoading={isExporting.value}
            disabled={isExporting.value}
          >
            导出SQL
          </Button>

          {onReset && (
            <Button
              variant="secondary"
              onClick={onReset}
              disabled={isExporting.value}
            >
              重新分析
            </Button>
          )}
        </div>
      </Card>

      <Card title="SQL预览">
        <div className="bg-gray-800 text-gray-200 p-4 rounded-md overflow-x-auto">
          <pre className="whitespace-pre-wrap break-words">{result.sql_preview}</pre>
        </div>
      </Card>
    </div>
  );
}
