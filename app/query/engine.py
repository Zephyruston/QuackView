import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from ..analyzer.excel_analyzer import ExcelAnalyzer
from ..connector.excel_connector import AnalysisMode, ExcelConnector
from ..executor.sql_executor import SQLExecutor
from ..generator.sql_generator import SQLGenerator

logger = logging.getLogger(__name__)


class DBEngine:
    """查询服务类"""

    def __init__(
        self,
        db_path: Optional[str] = None,
        mode: AnalysisMode = AnalysisMode.PERSISTENT,
    ):
        """
        初始化查询服务

        Args:
            db_path: DuckDB数据库文件路径
            mode: 分析模式
        """
        self.connector = ExcelConnector(db_path, mode)
        self.analyzer = ExcelAnalyzer(db_path, mode)
        self.current_table: Optional[str] = None
        self.executor: Optional[SQLExecutor] = None
        self.generator: Optional[SQLGenerator] = None

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.connector.close()

    def import_excel(
        self,
        excel_path: str,
        table_name: Optional[str] = None,
        sheet_name: Optional[Union[str, int]] = 0,
        **pandas_kwargs,
    ) -> Dict[str, Any]:
        """
        导入Excel文件并初始化分析环境

        Args:
            excel_path: Excel文件路径
            table_name: 表名
            sheet_name: 工作表名称或索引
            **pandas_kwargs: 传递给pandas.read_excel的参数

        Returns:
            导入结果字典
        """
        try:
            table_name = self.connector.import_excel(
                excel_path, table_name, sheet_name, **pandas_kwargs
            )
            self.current_table = table_name

            self.executor = SQLExecutor(self.connector.connect())
            self.generator = SQLGenerator(self.connector.connect(), table_name)

            analysis_info = self.analyzer.import_and_analyze(
                excel_path, table_name, sheet_name, **pandas_kwargs
            )

            return {
                "success": True,
                "table_name": table_name,
                "analysis_info": analysis_info,
                "error": None,
            }

        except Exception as e:
            logger.error(f"导入Excel文件失败: {e}")
            return {
                "success": False,
                "table_name": None,
                "analysis_info": None,
                "error": str(e),
            }

    def import_dataframe(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        导入DataFrame并初始化分析环境

        Args:
            df: 要导入的DataFrame
            table_name: 表名

        Returns:
            导入结果字典
        """
        try:
            table_name = self.connector.import_dataframe(df, table_name)
            self.current_table = table_name

            self.executor = SQLExecutor(self.connector.connect())
            self.generator = SQLGenerator(self.connector.connect(), table_name)

            analysis_info = self.analyzer.import_dataframe_and_analyze(df, table_name)

            return {
                "success": True,
                "table_name": table_name,
                "analysis_info": analysis_info,
                "error": None,
            }

        except Exception as e:
            logger.error(f"导入DataFrame失败: {e}")
            return {
                "success": False,
                "table_name": None,
                "analysis_info": None,
                "error": str(e),
            }

    def execute_analysis(
        self,
        column_name: str,
        analysis_type: str,
        group_by_columns: Optional[List[str]] = None,
        where_conditions: Optional[Dict[str, Union[str, Tuple, List]]] = None,
        limit: Optional[int] = None,
        top_k: Optional[int] = 10,
    ) -> Dict[str, Any]:
        """
        执行单字段分析

        Args:
            column_name: 要分析的字段名
            analysis_type: 分析类型，支持以下类型：

                数值分析:
                - "avg": 计算平均值
                - "max": 计算最大值
                - "min": 计算最小值
                - "sum": 计算总和
                - "var_pop": 计算总体方差
                - "stddev_pop": 计算总体标准差
                - "count": 计算数量

                字符串分析:
                - "distinct_count": 计算不同值数量
                - "top_k": 获取出现次数最多的K个值 (默认: 10)
                - "value_distribution": 计算值的分布

                时间分析:
                - "date_range": 计算日期范围 (最小值/最大值)
                - "year_analysis": 按年聚合
                - "month_analysis": 按月聚合
                - "day_analysis": 按天聚合

            group_by_columns: 分组字段列表
            where_conditions: WHERE条件字典
            limit: 限制结果数量
            top_k: TOP-K分析时的K值

        Returns:
            分析结果字典
        """
        if not self.executor or not self.current_table:
            return {
                "success": False,
                "result": None,
                "sql": None,
                "error": "请先导入Excel文件",
            }

        return self.executor.execute_analysis(
            table_name=self.current_table,
            column_name=column_name,
            analysis_type=analysis_type,
            group_by_columns=group_by_columns,
            where_conditions=where_conditions,
            limit=limit,
            top_k=top_k,
        )

    def execute_multi_column_analysis(
        self,
        analysis_config: Dict[str, str],
        group_by_columns: Optional[List[str]] = None,
        where_conditions: Optional[Dict[str, Union[str, Tuple, List]]] = None,
    ) -> Dict[str, Any]:
        """
        执行多字段分析

        Args:
            analysis_config: 分析配置 {字段名: 分析类型字符串}
            group_by_columns: 分组字段列表
            where_conditions: WHERE条件字典

        Returns:
            分析结果字典
        """
        if not self.executor or not self.current_table:
            return {
                "success": False,
                "result": None,
                "sql": None,
                "error": "请先导入Excel文件",
            }

        return self.executor.execute_multi_column_analysis(
            table_name=self.current_table,
            analysis_config=analysis_config,
            group_by_columns=group_by_columns,
            where_conditions=where_conditions,
        )

    def get_available_analyses(self, column_name: str) -> List[str]:
        """
        获取字段可用的分析方法

        Args:
            column_name: 字段名

        Returns:
            可用分析方法列表
        """
        if not self.current_table:
            return []

        return self.analyzer.get_available_analyses(column_name)

    def get_default_analysis(self, column_name: str) -> str:
        """
        获取字段的默认分析方法

        Args:
            column_name: 字段名

        Returns:
            默认分析方法
        """
        if not self.current_table:
            return "count"

        return self.analyzer.get_default_analysis(column_name)

    def get_table_info(self) -> Dict[str, Any]:
        """
        获取当前表信息

        Returns:
            表信息字典
        """
        if not self.current_table:
            return {}

        return self.connector.get_table_info(self.current_table)

    def get_sample_data(self, limit: int = 5) -> pd.DataFrame:
        """
        获取当前表样本数据

        Args:
            limit: 限制返回行数

        Returns:
            样本数据DataFrame
        """
        if not self.current_table:
            return pd.DataFrame()

        return self.connector.get_sample_data(self.current_table, limit)

    def get_column_types(self) -> Dict[str, str]:
        """
        获取当前表的列类型映射

        Returns:
            列名到类型的映射字典
        """
        if not self.current_table:
            return {}

        return self.connector.get_column_types(self.current_table)

    def execute_custom_sql(self, sql: str) -> Dict[str, Any]:
        """
        执行自定义SQL查询

        Args:
            sql: SQL查询语句

        Returns:
            查询结果字典
        """
        if not self.executor:
            return {
                "success": False,
                "result": None,
                "sql": sql,
                "error": "请先导入Excel文件",
            }

        try:
            result = self.executor.execute(sql)
            return {"success": True, "result": result, "sql": sql, "error": None}
        except Exception as e:
            return {"success": False, "result": None, "sql": sql, "error": str(e)}

    def get_quick_analysis(self) -> Dict[str, Any]:
        """
        获取快速分析结果

        Returns:
            快速分析结果
        """
        if not self.current_table:
            return {}

        return self.analyzer.get_quick_analysis()

    def close(self):
        """关闭连接"""
        self.connector.close()


def create_memory_query_service() -> DBEngine:
    """创建内存模式查询服务"""
    return DBEngine(mode=AnalysisMode.MEMORY)


def create_persistent_query_service(db_path: str = "quackview.duckdb") -> DBEngine:
    """创建持久化模式查询服务"""
    return DBEngine(db_path=db_path, mode=AnalysisMode.PERSISTENT)
