import logging

import uvicorn
from fastapi import FastAPI

from .config import config
from .middleware import setup_middleware
from .routes import router


def setup_logging():
    """配置日志"""
    import os

    log_dir = config.log_dir
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(log_dir, config.log_file)

    log_format = config.log_format

    file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    file_handler.setLevel(getattr(logging, config.log_level))
    file_handler.setFormatter(logging.Formatter(log_format))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, config.log_level))
    console_handler.setFormatter(logging.Formatter(log_format))

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.log_level))
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logger = logging.getLogger(__name__)
    logger.info("QuackView API 服务启动")
    logger.info(f"日志文件路径: {os.path.abspath(log_file)}")
    logger.info(
        f"配置信息: CORS={config.cors_allow_origins}, 日志级别={config.log_level}"
    )


def create_app() -> FastAPI:
    """创建FastAPI应用"""
    setup_logging()

    app = FastAPI(
        title=config.title,
        description=config.description,
        version=config.version,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    setup_middleware(app)

    app.include_router(router, prefix="")

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "app.api.main:app", host=config.host, port=config.port, reload=config.reload
    )
