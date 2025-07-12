"""
QuackView API 中间件
包含错误处理中间件、日志中间件和CORS中间件
"""

import logging
import time
import traceback
from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .config import config
from .exceptions import QuackViewException
from .models import ErrorResponse

logger = logging.getLogger(__name__)


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """统一错误处理中间件"""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        try:
            logger.info(f"[Middleware] {request.method} {request.url.path} - 开始处理")

            response = await call_next(request)

            process_time = time.time() - start_time
            logger.info(
                f"[Middleware] {request.method} {request.url.path} - 完成处理 "
                f"(耗时: {process_time:.3f}s, 状态码: {response.status_code})"
            )

            return response

        except QuackViewException as e:
            process_time = time.time() - start_time
            logger.error(
                f"[Middleware] {request.method} {request.url.path} - 业务异常 "
                f"(耗时: {process_time:.3f}s): {e.error_code} - {e.message}",
                extra={
                    "error_code": e.error_code,
                    "status_code": e.status_code,
                    "details": e.details,
                    "task_id": e.details.get("task_id") if e.details else None,
                },
            )

            return JSONResponse(
                status_code=e.status_code,
                content=ErrorResponse(error=e.error_code, detail=e.message).dict(),
            )

        except Exception as e:
            process_time = time.time() - start_time
            logger.error(
                f"[Middleware] {request.method} {request.url.path} - 未预期异常 "
                f"(耗时: {process_time:.3f}s): {str(e)}",
                exc_info=True,
                extra={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc(),
                },
            )

            return JSONResponse(
                status_code=500,
                content=ErrorResponse(
                    error="INTERNAL_SERVER_ERROR", detail="An unexpected error occurred"
                ).model_dump(),
            )


class LoggingMiddleware(BaseHTTPMiddleware):
    """请求日志中间件"""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")

        logger.info(
            f"[Request] {request.method} {request.url.path} "
            f"from {client_ip} - User-Agent: {user_agent}"
        )

        response = await call_next(request)

        logger.info(
            f"[Response] {request.method} {request.url.path} "
            f"- Status: {response.status_code}"
        )

        return response


def setup_cors_middleware(app: FastAPI):
    """设置CORS中间件"""
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.cors_allow_origins,
        allow_credentials=config.cors_allow_credentials,
        allow_methods=config.cors_allow_methods,
        allow_headers=config.cors_allow_headers,
    )


def setup_middleware(app: FastAPI):
    """设置所有中间件"""
    setup_cors_middleware(app)

    app.add_middleware(ErrorHandlingMiddleware)
    app.add_middleware(LoggingMiddleware)
