import { useSignal } from "@preact/signals";
import { Button } from "../components/Button.tsx";
import { Alert } from "../components/Alert.tsx";
import { apiClient } from "../utils/api-client.ts";

interface FileUploadProps {
  onSuccess?: (taskId: string) => void;
}

export default function FileUpload({ onSuccess }: FileUploadProps) {
  const isUploading = useSignal(false);
  const error = useSignal<string | null>(null);
  const success = useSignal<string | null>(null);
  const fileName = useSignal<string | null>(null);

  console.log("[FileUpload] 组件初始化，onSuccess:", onSuccess);

  const handleFileChange = (e: Event) => {
    const input = e.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      fileName.value = input.files[0].name;
      error.value = null;
    }
  };

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    console.log("[FileUpload] 开始处理文件上传");

    const form = e.target as HTMLFormElement;
    const _formData = new FormData(form);
    const fileInput = form.querySelector(
      'input[type="file"]',
    ) as HTMLInputElement;

    if (!fileInput.files || fileInput.files.length === 0) {
      error.value = "请选择一个Excel文件";
      console.log("[FileUpload] 没有选择文件");
      return;
    }

    const file = fileInput.files[0];
    console.log("[FileUpload] 选择的文件:", file.name, "大小:", file.size);

    if (!file.name.endsWith(".xlsx") && !file.name.endsWith(".xls")) {
      error.value = "请上传Excel文件 (.xlsx 或 .xls)";
      console.log("[FileUpload] 文件格式不支持:", file.name);
      return;
    }

    try {
      isUploading.value = true;
      error.value = null;
      success.value = null;

      console.log("[FileUpload] 发送上传请求到 /api/connection");
      const data = await apiClient.createConnection(file);
      console.log("[FileUpload] 上传成功，返回数据:", data);

      success.value = "文件上传成功！";
      fileName.value = null;
      form.reset();

      console.log("[FileUpload] onSuccess 回调检查:", {
        onSuccess: typeof onSuccess,
        taskId: data.task_id,
        hasTaskId: !!data.task_id,
      });

      if (onSuccess && data.task_id) {
        console.log("[FileUpload] 调用 onSuccess 回调，task_id:", data.task_id);
        onSuccess(data.task_id);
      } else {
        console.warn("[FileUpload] onSuccess 回调未定义或 task_id 不存在");
        console.warn("[FileUpload] onSuccess 类型:", typeof onSuccess);
        console.warn("[FileUpload] task_id 值:", data.task_id);

        if (data.task_id) {
          console.log("[FileUpload] 直接执行页面跳转");
          setTimeout(() => {
            try {
              globalThis.location.href = `/analyze?task_id=${data.task_id}`;
            } catch (error) {
              console.error("[FileUpload] 直接跳转失败:", error);
            }
          }, 100);
        }
      }
    } catch (err) {
      console.error("[FileUpload] 上传过程中发生错误:", err);
      error.value = err instanceof Error ? err.message : "上传过程中发生错误";
    } finally {
      isUploading.value = false;
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

      {success.value && (
        <Alert
          type="success"
          message={success.value}
          className="mb-4"
          onClose={() => success.value = null}
        />
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="flex items-center justify-center w-full">
          <label className="flex flex-col items-center justify-center w-full h-64 border-2 border-gray-300 border-dashed rounded-lg cursor-pointer bg-gray-50 hover:bg-gray-100">
            <div className="flex flex-col items-center justify-center pt-5 pb-6">
              <svg
                className="w-10 h-10 mb-3 text-gray-400"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="2"
                  d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
                >
                </path>
              </svg>
              <p className="mb-2 text-sm text-gray-500">
                <span className="font-semibold">点击上传</span> 或拖放
              </p>
              <p className="text-xs text-gray-500">Excel文件 (XLSX, XLS)</p>
              {fileName.value && (
                <p className="mt-2 text-sm text-blue-600">{fileName.value}</p>
              )}
            </div>
            <input
              type="file"
              name="file"
              className="hidden"
              accept=".xlsx,.xls"
              onChange={handleFileChange}
            />
          </label>
        </div>

        <div className="flex justify-center">
          <Button
            type="submit"
            variant="primary"
            size="lg"
            isLoading={isUploading.value}
            disabled={isUploading.value}
          >
            上传并分析
          </Button>
        </div>
      </form>
    </div>
  );
}
