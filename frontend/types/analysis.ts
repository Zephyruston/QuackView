import type { components } from "../api.d.ts";

export type AnalysisResult = components["schemas"]["AnalysisResult"];
export type Schema = components["schemas"]["Schema"];
export type ColumnInfo = components["schemas"]["ColumnInfo"];
export type AnalysisOperation = components["schemas"]["AnalysisOperation"];
export type FilterCondition = components["schemas"]["FilterCondition"];
export type AnalysisRequest = components["schemas"]["AnalysisRequest"];
export type CustomQueryRequest = components["schemas"]["CustomQueryRequest"];
export type ConnectionResponse = components["schemas"]["ConnectionResponse"];
export type AnalysisOptionsResponse =
  components["schemas"]["AnalysisOptionsResponse"];

export interface AnalysisFormOperation {
  type: string;
  field: string;
  alias: string;
}

export interface AnalysisFormFilter {
  field: string;
  operator: string;
  value: string;
}

export interface AnalysisFormGroupBy {
  field: string;
}

export interface AnalysisOption {
  name: string;
  description: string;
  type: string;
  fields: string[];
}

export interface SessionInfo {
  table_name: string;
  [key: string]: unknown;
}

export interface HealthCheckResponse {
  status: string;
  [key: string]: unknown;
}

export type AnalysisResultRows = unknown[][];

export type SchemaColumns = components["schemas"]["ColumnInfo"][];
