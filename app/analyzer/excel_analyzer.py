import logging
from enum import Enum
from typing import Dict, List, Optional, Union

import pandas as pd

from ..connector.excel_connector import AnalysisMode, ExcelConnector

logger = logging.getLogger(__name__)


class AnalysisType(Enum):
    """分析操作类型枚举"""

    # 数值字段分析
    AVG = "avg"  # 平均值
    MAX = "max"  # 最大值
    MIN = "min"  # 最小值
    SUM = "sum"  # 总和
    VAR_POP = "var_pop"  # 总体方差
    STDDEV_POP = "stddev_pop"  # 总体标准差
    COUNT = "count"  # 计数
    MEDIAN = "median"  # 中位数
    QUARTILES = "quartiles"  # 四分位数
    PERCENTILES = "percentiles"  # 百分位数

    # 字符串字段分析
    DISTINCT_COUNT = "distinct_count"  # 唯一值计数
    TOP_K = "top_k"  # 前K个最常见值
    VALUE_DISTRIBUTION = "value_distribution"  # 值分布
    LENGTH_ANALYSIS = "length_analysis"  # 字符串长度分析
    PATTERN_ANALYSIS = "pattern_analysis"  # 模式分析

    # 时间字段分析
    DATE_RANGE = "date_range"  # 日期范围
    YEAR_ANALYSIS = "year_analysis"  # 年度分析
    MONTH_ANALYSIS = "month_analysis"  # 月度分析
    DAY_ANALYSIS = "day_analysis"  # 日分析
    HOUR_ANALYSIS = "hour_analysis"  # 小时分析
    WEEKDAY_ANALYSIS = "weekday_analysis"  # 星期分析
    SEASONAL_ANALYSIS = "seasonal_analysis"  # 季节性分析

    # 通用分析
    MISSING_VALUES = "missing_values"  # 缺失值分析
    DATA_QUALITY = "data_quality"  # 数据质量检查
    CORRELATION = "correlation"  # 相关性分析


class ExcelAnalyzer:
    """Excel分析器, 负责分析能力定义和规则管理"""

    def __init__(
        self,
        db_path: Optional[str] = None,
        mode: AnalysisMode = AnalysisMode.PERSISTENT,
    ):
        """
        初始化Excel分析器

        Args:
            db_path: DuckDB数据库文件路径, 内存模式时可为None
            mode: 分析模式, MEMORY或PERSISTENT
        """
        self.connector = ExcelConnector(db_path, mode)
        self.current_table: Optional[str] = None
        self.mode = mode

        self.analysis_rules = {
            "INTEGER": [
                "sum",
                "avg",
                "max",
                "min",
                "count",
                "median",
                "quartiles",
                "percentiles",
            ],
            "BIGINT": [
                "sum",
                "avg",
                "max",
                "min",
                "count",
                "median",
                "quartiles",
                "percentiles",
            ],
            "DOUBLE": [
                "sum",
                "avg",
                "max",
                "min",
                "count",
                "median",
                "quartiles",
                "percentiles",
            ],
            "FLOAT": [
                "sum",
                "avg",
                "max",
                "min",
                "count",
                "median",
                "quartiles",
                "percentiles",
            ],
            "REAL": [
                "sum",
                "avg",
                "max",
                "min",
                "count",
                "median",
                "quartiles",
                "percentiles",
            ],
            "DECIMAL": [
                "sum",
                "avg",
                "max",
                "min",
                "count",
                "median",
                "quartiles",
                "percentiles",
            ],
            "VARCHAR": [
                "count",
                "distinct_count",
                "top_k",
                "value_distribution",
                "length_analysis",
                "pattern_analysis",
            ],
            "TEXT": [
                "count",
                "distinct_count",
                "top_k",
                "value_distribution",
                "length_analysis",
                "pattern_analysis",
            ],
            "STRING": [
                "count",
                "distinct_count",
                "top_k",
                "value_distribution",
                "length_analysis",
                "pattern_analysis",
            ],
            "TIMESTAMP": [
                "count",
                "date_range",
                "year_analysis",
                "month_analysis",
                "day_analysis",
                "hour_analysis",
                "weekday_analysis",
                "seasonal_analysis",
            ],
            "TIMESTAMP_NS": [
                "count",
                "date_range",
                "year_analysis",
                "month_analysis",
                "day_analysis",
                "hour_analysis",
                "weekday_analysis",
                "seasonal_analysis",
            ],
        }

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口, 自动关闭连接"""
        self.connector.close()

    def get_mode_info(self) -> Dict[str, str]:
        """
        获取模式信息

        Returns:
            模式信息字典
        """
        return self.connector.get_mode_info()

    def get_available_analyses(self, column: str) -> List[str]:
        """
        获取某字段可用的分析方法

        Args:
            column: 字段名

        Returns:
            可用分析方法列表
        """
        if self.current_table is None:
            raise ValueError("没有当前表, 请先导入Excel文件")

        schema = self.connector.get_column_types(self.current_table)
        col_type = schema.get(column, "varchar")
        col_type_upper = col_type.upper()
        return self.analysis_rules.get(col_type_upper, ["count"])

    def get_default_analysis(self, column: str) -> str:
        """
        获取字段的默认分析方法

        Args:
            column: 字段名

        Returns:
            分析方法字符串
        """
        if self.current_table is None:
            raise ValueError("没有当前表, 请先导入Excel文件")

        col_type = self.connector.get_column_types(self.current_table).get(
            column, "varchar"
        )
        col_type_upper = col_type.upper()

        if col_type_upper in [
            "INTEGER",
            "BIGINT",
            "DOUBLE",
            "FLOAT",
            "REAL",
            "DECIMAL",
        ]:
            return "avg"
        elif col_type_upper in ["VARCHAR", "TEXT", "STRING"]:
            return "count"
        elif col_type_upper in ["TIMESTAMP", "TIMESTAMP_NS", "DATE", "TIME"]:
            return "count"
        return "count"

    def import_and_analyze(
        self,
        excel_path: str,
        table_name: Optional[str] = None,
        sheet_name: Optional[Union[str, int]] = 0,
        **pandas_kwargs,
    ) -> Dict:
        """
        导入Excel文件并返回分析信息

        Args:
            excel_path: Excel文件路径
            table_name: 表名
            sheet_name: 工作表名称或索引
            **pandas_kwargs: 传递给pandas.read_excel的参数

        Returns:
            包含表信息和可用分析的字典
        """
        logger.info(f"[Analyzer] 开始分析Excel: {excel_path}, table_name={table_name}")
        try:
            table_name = self.connector.import_excel(
                excel_path, table_name, sheet_name, **pandas_kwargs
            )
            self.current_table = table_name
            logger.info(f"[Analyzer] Excel导入并建表成功: {table_name}")
        except Exception as e:
            logger.error(f"[Analyzer] Excel导入或建表失败: {e}", exc_info=True)
            raise

        try:
            table_info = self.connector.get_table_info(table_name)
            logger.info(f"[Analyzer] 获取表信息成功: {table_info.get('table_name')}")
        except Exception as e:
            logger.error(f"[Analyzer] 获取表信息失败: {e}", exc_info=True)
            raise

        try:
            column_types = self.connector.get_column_types(table_name)
            logger.info(f"[Analyzer] 获取列类型成功: {column_types}")
        except Exception as e:
            logger.error(f"[Analyzer] 获取列类型失败: {e}", exc_info=True)
            raise

        analysis_options = {}
        for column_name, column_type in column_types.items():
            available_types = self.get_available_analyses(column_name)
            analysis_options[column_name] = [
                {
                    "type": analysis_type,
                    "description": self._get_analysis_description(analysis_type),
                }
                for analysis_type in available_types
            ]

        try:
            sample_data = self.connector.get_sample_data(table_name).to_dict("records")
            logger.info(f"[Analyzer] 获取样本数据成功, 行数: {len(sample_data)}")
        except Exception as e:
            logger.error(f"[Analyzer] 获取样本数据失败: {e}", exc_info=True)
            sample_data = []

        mode_info = self.get_mode_info()
        logger.info(f"[Analyzer] 模式信息: {mode_info}")

        return {
            "table_info": table_info,
            "column_types": column_types,
            "analysis_options": analysis_options,
            "sample_data": sample_data,
            "mode_info": mode_info,
        }

    def import_dataframe_and_analyze(self, df: pd.DataFrame, table_name: str) -> Dict:
        """
        导入DataFrame并返回分析信息

        Args:
            df: 要导入的DataFrame
            table_name: 表名

        Returns:
            包含表信息和可用分析的字典
        """
        table_name = self.connector.import_dataframe(df, table_name)
        self.current_table = table_name

        table_info = self.connector.get_table_info(table_name)

        column_types = self.connector.get_column_types(table_name)

        analysis_options = {}
        for column_name, column_type in column_types.items():
            available_types = self.get_available_analyses(column_name)
            analysis_options[column_name] = [
                {
                    "type": analysis_type,
                    "description": self._get_analysis_description(analysis_type),
                }
                for analysis_type in available_types
            ]

        return {
            "table_info": table_info,
            "column_types": column_types,
            "analysis_options": analysis_options,
            "sample_data": self.connector.get_sample_data(table_name).to_dict(
                "records"
            ),
            "mode_info": self.get_mode_info(),
        }

    def get_quick_analysis(self) -> Dict:
        """
        获取快速分析结果（所有数值字段的平均值）

        Returns:
            快速分析结果
        """
        if self.current_table is None:
            raise ValueError("没有当前表, 请先导入Excel文件")

        column_types = self.connector.get_column_types(self.current_table)

        results = {}
        for column_name, column_type in column_types.items():
            if column_type in [
                "integer",
                "bigint",
                "double",
                "float",
                "real",
                "decimal",
            ]:
                try:
                    from ..executor.sql_executor import SQLExecutor
                    from ..generator.sql_generator import AnalysisType, SQLGenerator

                    generator = SQLGenerator(
                        self.connector.connect(), self.current_table
                    )
                    executor = SQLExecutor(self.connector.connect())

                    sql = generator.generate_sql(column_name, AnalysisType.AVG)
                    result = executor.execute(sql)

                    if not result.empty:
                        results[column_name] = {
                            "type": "avg",
                            "value": result.iloc[0, 0],
                            "sql": sql,
                        }
                except Exception as e:
                    logger.warning(f"分析字段 {column_name} 时出错: {e}")

        return results

    def _get_analysis_description(self, analysis_type: str) -> str:
        """
        获取分析类型的描述

        Args:
            analysis_type: 分析类型字符串

        Returns:
            分析类型的描述字符串
        """
        descriptions = {
            # 数值分析
            "avg": "计算平均值",
            "max": "获取最大值",
            "min": "获取最小值",
            "sum": "计算总和",
            "count": "计算记录数",
            "var_pop": "计算总体方差",
            "stddev_pop": "计算总体标准差",
            "median": "计算中位数",
            "quartiles": "计算四分位数",
            "percentiles": "计算百分位数",
            # 字符串分析
            "distinct_count": "统计唯一值数量",
            "top_k": "获取前K个最常见值",
            "value_distribution": "分析值分布",
            "length_analysis": "分析字符串长度",
            "pattern_analysis": "模式识别分析",
            # 时间分析
            "date_range": "计算日期范围",
            "year_analysis": "按年度分析",
            "month_analysis": "按月度分析",
            "day_analysis": "按日期分析",
            "hour_analysis": "按小时分析",
            "weekday_analysis": "按星期分析",
            "seasonal_analysis": "季节性分析",
            # 通用分析
            "missing_values": "缺失值分析",
            "data_quality": "数据质量检查",
            "correlation": "相关性分析",
        }
        return descriptions.get(analysis_type, f"{analysis_type}分析")


def create_memory_analyzer() -> ExcelAnalyzer:
    """创建内存模式分析器"""
    return ExcelAnalyzer(mode=AnalysisMode.MEMORY)


def create_persistent_analyzer(db_path: str = "quackview.duckdb") -> ExcelAnalyzer:
    """创建持久化模式分析器"""
    return ExcelAnalyzer(db_path=db_path, mode=AnalysisMode.PERSISTENT)
