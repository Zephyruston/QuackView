import logging
import os
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalysisMode(Enum):
    """分析模式枚举"""

    MEMORY = "memory"
    PERSISTENT = "persistent"


class ExcelConnector:
    """Excel文件连接器, 负责Excel文件导入DuckDB和连接管理"""

    def __init__(
        self,
        db_path: Optional[str] = None,
        mode: AnalysisMode = AnalysisMode.PERSISTENT,
    ):
        """
        初始化Excel连接器

        Args:
            db_path: DuckDB数据库文件路径, 内存模式时可为None
            mode: 分析模式, MEMORY(内存模式)或PERSISTENT(持久化模式)
        """
        self.mode = mode

        if mode == AnalysisMode.MEMORY:
            self.db_path = ":memory:"
            logger.info("使用内存模式进行分析")
        else:
            self.db_path = db_path or "quackview.duckdb"
            logger.info(f"使用持久化模式, 数据库文件: {self.db_path}")

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.current_table_name: Optional[str] = None

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口, 自动关闭连接"""
        self.close()

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        连接到DuckDB数据库

        Returns:
            DuckDB连接对象
        """
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path)
            if self.mode == AnalysisMode.MEMORY:
                logger.info("已连接到内存数据库")
            else:
                logger.info(f"已连接到数据库: {self.db_path}")

        return self.conn

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("数据库连接已关闭")
            except Exception as e:
                logger.warning(f"关闭连接时出现错误: {e}")
            finally:
                self.conn = None

    def get_mode_info(self) -> Dict[str, str]:
        """
        获取模式信息

        Returns:
            模式信息字典
        """
        return {
            "mode": self.mode.value,
            "db_path": self.db_path,
            "description": (
                "内存模式" if self.mode == AnalysisMode.MEMORY else "持久化模式"
            ),
        }

    def import_excel(
        self,
        excel_path: str,
        table_name: Optional[str] = None,
        sheet_name: Optional[Union[str, int]] = 0,
        **pandas_kwargs,
    ) -> str:
        """
        导入Excel文件到DuckDB

        Args:
            excel_path: Excel文件路径
            table_name: 表名, 如果为None则使用文件名
            sheet_name: 工作表名称或索引, 默认为第一个工作表
            **pandas_kwargs: 传递给pandas.read_excel的参数

        Returns:
            创建的表名
        """
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel文件不存在: {excel_path}")

        if table_name is None:
            table_name = Path(excel_path).stem.lower()
            table_name = "".join(c for c in table_name if c.isalnum() or c == "_")

        logger.info(f"正在读取Excel文件: {excel_path}")
        try:
            df = pd.read_excel(excel_path, sheet_name=sheet_name, **pandas_kwargs)
        except Exception as e:
            raise ValueError(f"读取Excel文件失败: {e}")

        return self.import_dataframe(df, table_name)

    def import_dataframe(self, df: pd.DataFrame, table_name: str) -> str:
        """
        直接导入DataFrame到数据库

        Args:
            df: 要导入的DataFrame
            table_name: 表名

        Returns:
            创建的表名
        """
        conn = self.connect()
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        self.current_table_name = table_name
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        column_count = len(df.columns)

        logger.info(f"成功导入DataFrame到表 '{table_name}'")
        logger.info(f"数据行数: {row_count}, 列数: {column_count}")

        return table_name

    def get_table_info(self, table_name: Optional[str] = None) -> Dict:
        """
        获取表信息

        Args:
            table_name: 表名, 如果为None则使用当前表

        Returns:
            表信息字典
        """
        if table_name is None:
            table_name = self.current_table_name

        if table_name is None:
            raise ValueError("未指定表名且没有当前表")

        conn = self.connect()

        schema_df = conn.execute(f"DESCRIBE {table_name}").fetch_df()
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        columns = []
        for _, row in schema_df.iterrows():
            columns.append(
                {
                    "name": row["column_name"],
                    "type": row["column_type"],
                    "null": row["null"],
                    "key": row["key"],
                    "default": row["default"],
                    "extra": row["extra"],
                }
            )

        return {
            "table_name": table_name,
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
        }

    def list_tables(self) -> List[str]:
        """
        获取所有表名列表

        Returns:
            表名列表
        """
        conn = self.connect()
        result = conn.execute("SHOW TABLES").fetch_df()
        return result["name"].tolist()

    def get_sample_data(
        self, table_name: Optional[str] = None, limit: int = 5
    ) -> pd.DataFrame:
        """
        获取表样本数据

        Args:
            table_name: 表名, 如果为None则使用当前表
            limit: 限制返回行数

        Returns:
            样本数据DataFrame
        """
        if table_name is None:
            table_name = self.current_table_name

        if table_name is None:
            raise ValueError("未指定表名且没有当前表")

        conn = self.connect()
        return conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetch_df()

    def execute_query(self, sql: str) -> pd.DataFrame:
        """
        执行SQL查询

        Args:
            sql: SQL查询语句

        Returns:
            查询结果DataFrame
        """
        conn = self.connect()
        return conn.execute(sql).fetch_df()

    def get_column_types(self, table_name: Optional[str] = None) -> Dict[str, str]:
        """
        获取表的列类型映射

        Args:
            table_name: 表名, 如果为None则使用当前表

        Returns:
            列名到类型的映射字典
        """
        if table_name is None:
            table_name = self.current_table_name

        if table_name is None:
            raise ValueError("未指定表名且没有当前表")

        conn = self.connect()
        describe_result = conn.execute(f"DESCRIBE {table_name}").fetch_df()
        type_map = dict(
            zip(
                describe_result["column_name"],
                describe_result["column_type"].str.lower(),
            )
        )
        return type_map

    def is_table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        conn = self.connect()
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
        return result > 0

    def drop_table(self, table_name: str):
        """
        删除表

        Args:
            table_name: 表名
        """
        conn = self.connect()
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        logger.info(f"已删除表: {table_name}")

        if table_name == self.current_table_name:
            self.current_table_name = None

    def export_to_file(self, file_path: str):
        """
        导出数据库到文件

        Args:
            file_path: 导出文件路径
        """
        if self.mode == AnalysisMode.MEMORY:
            logger.warning("内存模式无法导出数据库")
            return

        conn = self.connect()
        conn.execute(f"EXPORT DATABASE '{file_path}'")


def create_memory_connector() -> ExcelConnector:
    """创建内存模式连接器"""
    return ExcelConnector(mode=AnalysisMode.MEMORY)


def create_persistent_connector(db_path: str = "quackview.duckdb") -> ExcelConnector:
    """创建持久化模式连接器"""
    return ExcelConnector(db_path=db_path, mode=AnalysisMode.PERSISTENT)
