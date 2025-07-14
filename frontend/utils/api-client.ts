import type { components, paths } from "../api.d.ts";
import type {
  AnalysisOptionsResponse,
  AnalysisRequest,
  AnalysisResult,
  ConnectionResponse,
  CustomQueryRequest,
  HealthCheckResponse,
  Schema,
  SessionInfo,
} from "../types/analysis.ts";
import { api as apiLogger } from "./logger.ts";

const API_BASE = "/api";

export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE) {
    this.baseUrl = baseUrl;
  }

  async createConnection(file: File): Promise<ConnectionResponse> {
    apiLogger.debug("开始创建连接，文件:", file.name);
    const formData = new FormData();
    formData.append("file", file);

    const response = await fetch(`${this.baseUrl}/connection`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const error = await response.json();
      apiLogger.error("创建连接失败:", error);
      throw new Error(error.detail || "上传失败");
    }

    const result = await response.json();
    apiLogger.info("连接创建成功，taskId:", result.task_id);
    return result;
  }

  async closeConnection(taskId: string): Promise<void> {
    apiLogger.debug("开始关闭连接，taskId:", taskId);
    const response = await fetch(
      `${this.baseUrl}/connection?task_id=${taskId}`,
      {
        method: "DELETE",
      },
    );

    if (!response.ok) {
      const error = await response.json();
      apiLogger.error("关闭连接失败:", error);
      throw new Error(error.detail || "关闭连接失败");
    }

    apiLogger.info("连接关闭成功，taskId:", taskId);
  }

  async getSchema(taskId: string): Promise<Schema> {
    apiLogger.debug("开始获取schema，taskId:", taskId);
    const response = await fetch(`${this.baseUrl}/schema?task_id=${taskId}`);

    if (!response.ok) {
      const error = await response.json();
      apiLogger.error("获取schema失败:", error);
      throw new Error(error.detail || "获取schema失败");
    }

    const result = await response.json();
    apiLogger.debug("Schema获取成功:", result);
    return result;
  }

  async getAnalysisOptions(taskId: string): Promise<AnalysisOptionsResponse> {
    apiLogger.debug("开始获取分析选项，taskId:", taskId);
    const response = await fetch(
      `${this.baseUrl}/analysis-options?task_id=${taskId}`,
    );

    if (!response.ok) {
      const error = await response.json();
      apiLogger.error("获取分析选项失败:", error);
      throw new Error(error.detail || "获取分析选项失败");
    }

    const result = await response.json();
    apiLogger.debug("分析选项获取成功:", result);
    return result;
  }

  async executeAnalysis(
    request: AnalysisRequest & { group_by?: string[] },
  ): Promise<AnalysisResult> {
    apiLogger.debug("开始执行分析，请求:", request);
    const response = await fetch(`${this.baseUrl}/analyze`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await response.json();
      apiLogger.error("分析执行失败:", error);
      throw new Error(error.detail || "分析执行失败");
    }

    const result = await response.json();
    apiLogger.info("分析执行成功，结果行数:", result.rows.length);
    return result;
  }

  async executeCustomQuery(
    request: CustomQueryRequest,
  ): Promise<AnalysisResult> {
    const response = await fetch(`${this.baseUrl}/query/custom`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "查询执行失败");
    }

    return response.json();
  }

  async exportSql(taskId: string): Promise<Blob> {
    const response = await fetch(
      `${this.baseUrl}/export/sql?task_id=${taskId}`,
    );

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "导出SQL失败");
    }

    return response.blob();
  }

  async exportExcel(taskId: string): Promise<Blob> {
    const response = await fetch(
      `${this.baseUrl}/export/excel?task_id=${taskId}`,
    );

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "导出Excel失败");
    }

    return response.blob();
  }

  async exportResultExcel(columns: string[], rows: unknown[][]): Promise<Blob> {
    const response = await fetch(`${this.baseUrl}/export/result-excel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ columns, rows }),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "导出Excel失败");
    }
    return response.blob();
  }

  async getSessionInfo(taskId: string): Promise<SessionInfo> {
    const response = await fetch(
      `${this.baseUrl}/session-info?task_id=${taskId}`,
    );

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "获取会话信息失败");
    }

    return response.json();
  }

  async healthCheck(): Promise<HealthCheckResponse> {
    const response = await fetch("/health");

    if (!response.ok) {
      throw new Error("健康检查失败");
    }

    return response.json();
  }
}

export const apiClient = new ApiClient();

export type { components, paths };
