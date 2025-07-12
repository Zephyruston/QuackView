"""
QuackView API 配置文件
"""

from typing import List

from pydantic_settings import BaseSettings


class APIConfig(BaseSettings):
    """API配置类"""

    # 应用信息
    title: str = "QuackView API"
    description: str = "Click & Analyze - RESTful API for Excel data analysis"
    version: str = "1.0.0"

    # 服务器配置
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = True

    # CORS配置
    cors_allow_origins: List[str] = ["*"]
    cors_allow_credentials: bool = True
    cors_allow_methods: List[str] = ["*"]
    cors_allow_headers: List[str] = ["*"]

    # 日志配置
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_dir: str = "logs"
    log_file: str = "quackview_api.log"

    # 文件上传配置
    max_file_size: int = 50 * 1024 * 1024  # 50MB
    allowed_file_types: List[str] = [".xlsx", ".xls"]

    # 会话配置
    session_timeout: int = 3600  # 1小时

    class Config:
        env_prefix = "QUACKVIEW_"


config = APIConfig()
