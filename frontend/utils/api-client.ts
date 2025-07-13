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

const API_BASE = "/api";

export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE) {
    this.baseUrl = baseUrl;
  }

  async createConnection(file: File): Promise<ConnectionResponse> {
    const formData = new FormData();
    formData.append("file", file);

    const response = await fetch(`${this.baseUrl}/connection`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "上传失败");
    }

    return response.json();
  }

  async closeConnection(taskId: string): Promise<void> {
    const response = await fetch(
      `${this.baseUrl}/connection?task_id=${taskId}`,
      {
        method: "DELETE",
      },
    );

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "关闭连接失败");
    }
  }

  async getSchema(taskId: string): Promise<Schema> {
    const response = await fetch(`${this.baseUrl}/schema?task_id=${taskId}`);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "获取schema失败");
    }

    return response.json();
  }

  async getAnalysisOptions(taskId: string): Promise<AnalysisOptionsResponse> {
    const response = await fetch(
      `${this.baseUrl}/analysis-options?task_id=${taskId}`,
    );

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "获取分析选项失败");
    }

    return response.json();
  }

  async executeAnalysis(request: AnalysisRequest): Promise<AnalysisResult> {
    const response = await fetch(`${this.baseUrl}/analyze`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || "分析执行失败");
    }

    return response.json();
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
