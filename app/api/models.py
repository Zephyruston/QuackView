from typing import Any, List, Optional, Union

from pydantic import BaseModel


class ColumnInfo(BaseModel):
    name: str
    type: str


class Schema(BaseModel):
    table_name: str
    columns: List[ColumnInfo]


class AnalysisOperation(BaseModel):
    column: str
    operation: str


class FilterCondition(BaseModel):
    column: str
    operator: str
    value: Union[str, int, float, List[Any]]


class SortByCondition(BaseModel):
    field: str
    order: str


class AnalysisRequest(BaseModel):
    task_id: str
    operations: List[AnalysisOperation]
    filters: Optional[List[FilterCondition]] = None
    group_by: Optional[List[str]] = None
    sort_by: Optional[List[SortByCondition]] = None
    limit: Optional[int] = None


class AnalysisResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    sql_preview: str


class ConnectionResponse(BaseModel):
    task_id: str


class AnalysisOptionsResponse(BaseModel):
    options: List[dict]  # [{"column": "sales", "operations": ["SUM", "AVG", "COUNT"]}]


class CustomQueryRequest(BaseModel):
    task_id: str
    sql: str


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
