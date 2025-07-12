import io
import logging

from fastapi import APIRouter, File, Query, UploadFile
from fastapi.responses import StreamingResponse

from .exceptions import FileValidationError, SessionNotFoundError
from .models import (
    AnalysisOptionsResponse,
    AnalysisRequest,
    AnalysisResult,
    ConnectionResponse,
    CustomQueryRequest,
    Schema,
)
from .service import service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/api/connection", response_model=ConnectionResponse)
async def create_connection(file: UploadFile = File(...)):
    """上传Excel文件并创建分析会话"""
    logger.info(f"[API] /api/connection 收到文件: {file.filename}")

    if not file.filename.endswith((".xlsx", ".xls")):
        logger.warning(f"[API] 文件类型不支持: {file.filename}")
        raise FileValidationError(
            "Only Excel files (.xlsx, .xls) are supported", file.filename
        )

    file_content = await file.read()
    logger.info(f"[API] 文件内容长度: {len(file_content)} 字节")

    task_id = service.create_session(file_content, file.filename)
    logger.info(f"[API] 会话创建成功, task_id={task_id}")

    return ConnectionResponse(task_id=task_id)


@router.delete("/api/connection")
async def close_connection(task_id: str = Query(...)):
    """关闭分析会话"""
    logger.info(f"[API] /api/connection DELETE, task_id={task_id}")

    success = service.close_session(task_id)
    if not success:
        logger.warning(f"[API] 会话不存在: {task_id}")
        raise SessionNotFoundError(task_id)

    logger.info(f"[API] 会话关闭成功: {task_id}")
    return {"message": "Session closed successfully"}


@router.get("/api/schema", response_model=Schema)
async def get_schema(task_id: str = Query(...)):
    """获取表结构信息"""
    schema_data = service.get_schema(task_id)
    return Schema(**schema_data)


@router.get("/api/analysis-options", response_model=AnalysisOptionsResponse)
async def get_analysis_options(task_id: str = Query(...)):
    """获取分析选项"""
    options = service.get_analysis_options(task_id)
    return AnalysisOptionsResponse(options=options)


@router.post("/api/analyze", response_model=AnalysisResult)
async def execute_analysis(request: AnalysisRequest):
    """执行分析任务"""
    operations = [
        {"column": op.column, "operation": op.operation} for op in request.operations
    ]
    filters = None
    if request.filters:
        filters = [
            {"column": f.column, "operator": f.operator, "value": f.value}
            for f in request.filters
        ]

    result = service.execute_analysis(request.task_id, operations, filters)
    return AnalysisResult(**result)


@router.post("/api/query/custom", response_model=AnalysisResult)
async def execute_custom_query(request: CustomQueryRequest):
    """执行自定义SQL查询"""
    result = service.execute_custom_query(request.task_id, request.sql)
    return AnalysisResult(**result)


@router.get("/api/export/sql")
async def export_sql(task_id: str = Query(...)):
    """导出SQL脚本"""
    sql_content = service.export_sql(task_id)

    sql_file = io.BytesIO(sql_content.encode("utf-8"))
    sql_file.seek(0)

    return StreamingResponse(
        sql_file,
        media_type="text/sql",
        headers={
            "Content-Disposition": f"attachment; filename=quackview_export_{task_id}.sql"
        },
    )


@router.get("/api/export/excel")
async def export_excel(task_id: str = Query(...)):
    """导出Excel文件"""
    excel_content = service.export_excel(task_id)

    excel_file = io.BytesIO(excel_content)
    excel_file.seek(0)

    return StreamingResponse(
        excel_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=quackview_export_{task_id}.xlsx"
        },
    )


@router.get("/api/session-info")
async def get_session_info(task_id: str = Query(...)):
    """获取会话信息，包括表名"""
    session_info = service.get_session_info(task_id)
    return session_info


@router.get("/health")
async def health_check():
    """健康检查端点"""
    return {"status": "healthy", "service": "QuackView API"}
