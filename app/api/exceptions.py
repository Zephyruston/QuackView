"""
QuackView API 自定义异常类
定义各种业务异常类型，用于统一错误处理
"""

from typing import Any, Dict, Optional


class QuackViewException(Exception):
    """QuackView API 基础异常类"""

    def __init__(
        self,
        message: str,
        error_code: str = None,
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.status_code = status_code
        self.details = details or {}


class SessionNotFoundError(QuackViewException):
    """会话不存在异常"""

    def __init__(self, task_id: str):
        super().__init__(
            message=f"Session {task_id} not found",
            error_code="SESSION_NOT_FOUND",
            status_code=404,
            details={"task_id": task_id},
        )


class FileValidationError(QuackViewException):
    """文件验证异常"""

    def __init__(self, message: str, filename: str = None):
        super().__init__(
            message=message,
            error_code="FILE_VALIDATION_ERROR",
            status_code=400,
            details={"filename": filename} if filename else {},
        )


class ExcelProcessingError(QuackViewException):
    """Excel处理异常"""

    def __init__(self, message: str, filename: str = None):
        super().__init__(
            message=message,
            error_code="EXCEL_PROCESSING_ERROR",
            status_code=422,
            details={"filename": filename} if filename else {},
        )


class SQLExecutionError(QuackViewException):
    """SQL执行异常"""

    def __init__(self, message: str, sql: str = None):
        super().__init__(
            message=message,
            error_code="SQL_EXECUTION_ERROR",
            status_code=422,
            details={"sql": sql} if sql else {},
        )


class AnalysisError(QuackViewException):
    """分析任务异常"""

    def __init__(self, message: str, task_id: str = None):
        super().__init__(
            message=message,
            error_code="ANALYSIS_ERROR",
            status_code=422,
            details={"task_id": task_id} if task_id else {},
        )


class ExportError(QuackViewException):
    """导出异常"""

    def __init__(self, message: str, export_type: str = None):
        super().__init__(
            message=message,
            error_code="EXPORT_ERROR",
            status_code=500,
            details={"export_type": export_type} if export_type else {},
        )


class DatabaseConnectionError(QuackViewException):
    """数据库连接异常"""

    def __init__(self, message: str):
        super().__init__(
            message=message, error_code="DATABASE_CONNECTION_ERROR", status_code=500
        )


class InvalidRequestError(QuackViewException):
    """无效请求异常"""

    def __init__(self, message: str, field: str = None):
        super().__init__(
            message=message,
            error_code="INVALID_REQUEST_ERROR",
            status_code=400,
            details={"field": field} if field else {},
        )
