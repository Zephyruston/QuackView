from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

import duckdb

from ..utils.utils import (
    get_column_type_map,
    is_duckdb_numeric_type,
    is_duckdb_text_type,
    is_duckdb_time_type,
)


class AnalysisType(Enum):
    """分析操作类型枚举"""

    # 数值字段分析
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    SUM = "sum"
    VAR_POP = "var_pop"
    STDDEV_POP = "stddev_pop"
    COUNT = "count"

    # 字符串字段分析
    DISTINCT_COUNT = "distinct_count"
    TOP_K = "top_k"
    VALUE_DISTRIBUTION = "value_distribution"

    # 时间字段分析
    DATE_RANGE = "date_range"
    YEAR_ANALYSIS = "year_analysis"
    MONTH_ANALYSIS = "month_analysis"
    DAY_ANALYSIS = "day_analysis"


class SQLGenerator:
    """SQL生成器类"""

    # SQL模板映射
    SQL_TEMPLATES = {
        AnalysisType.AVG: "SELECT AVG({column}) as avg_{column}",
        AnalysisType.MAX: "SELECT MAX({column}) as max_{column}",
        AnalysisType.MIN: "SELECT MIN({column}) as min_{column}",
        AnalysisType.SUM: "SELECT SUM({column}) as sum_{column}",
        AnalysisType.VAR_POP: "SELECT VAR_POP({column}) as var_pop_{column}",
        AnalysisType.STDDEV_POP: "SELECT STDDEV_POP({column}) as stddev_pop_{column}",
        AnalysisType.COUNT: "SELECT COUNT({column}) as count_{column}",
        AnalysisType.DISTINCT_COUNT: "SELECT COUNT(DISTINCT {column}) as distinct_count_{column}",
        AnalysisType.TOP_K: "SELECT {column}, COUNT(*) as count FROM {table} GROUP BY {column}",
        AnalysisType.VALUE_DISTRIBUTION: "SELECT {column}, COUNT(*) as count FROM {table} GROUP BY {column}",
        AnalysisType.DATE_RANGE: "SELECT MIN({column}) as min_date, MAX({column}) as max_date",
        AnalysisType.YEAR_ANALYSIS: "SELECT EXTRACT(YEAR FROM {column}) as year, COUNT(*) as count",
        AnalysisType.MONTH_ANALYSIS: "SELECT EXTRACT(MONTH FROM {column}) as month, COUNT(*) as count",
        AnalysisType.DAY_ANALYSIS: "SELECT EXTRACT(DAY FROM {column}) as day, COUNT(*) as count",
    }

    # 分析类型描述映射
    ANALYSIS_DESCRIPTIONS = {
        AnalysisType.AVG: "计算平均值",
        AnalysisType.MAX: "获取最大值",
        AnalysisType.MIN: "获取最小值",
        AnalysisType.SUM: "计算总和",
        AnalysisType.VAR_POP: "计算方差",
        AnalysisType.STDDEV_POP: "计算标准差",
        AnalysisType.COUNT: "计算记录数",
        AnalysisType.DISTINCT_COUNT: "统计唯一值数量",
        AnalysisType.TOP_K: "获取前K个值",
        AnalysisType.VALUE_DISTRIBUTION: "值分布统计",
        AnalysisType.DATE_RANGE: "时间范围分析",
        AnalysisType.YEAR_ANALYSIS: "按年分析",
        AnalysisType.MONTH_ANALYSIS: "按月分析",
        AnalysisType.DAY_ANALYSIS: "按日分析",
    }

    def __init__(self, conn: duckdb.DuckDBPyConnection, table_name: str):
        """
        初始化SQL生成器

        Args:
            conn: DuckDB连接对象
            table_name: 表名
        """
        self.conn = conn
        self.table_name = table_name
        self.column_types = get_column_type_map(conn, table_name)

    def get_available_analysis_types(self, column_name: str) -> List[AnalysisType]:
        """
        获取指定字段可用的分析类型

        Args:
            column_name: 字段名

        Returns:
            可用的分析类型列表
        """
        if column_name not in self.column_types:
            return []

        column_type = self.column_types[column_name]
        available_types = []

        if is_duckdb_numeric_type(column_type):
            available_types.extend(
                [
                    AnalysisType.AVG,
                    AnalysisType.MAX,
                    AnalysisType.MIN,
                    AnalysisType.SUM,
                    AnalysisType.VAR_POP,
                    AnalysisType.STDDEV_POP,
                    AnalysisType.COUNT,
                ]
            )
        elif is_duckdb_text_type(column_type):
            available_types.extend(
                [
                    AnalysisType.COUNT,
                    AnalysisType.DISTINCT_COUNT,
                    AnalysisType.TOP_K,
                    AnalysisType.VALUE_DISTRIBUTION,
                ]
            )
        elif is_duckdb_time_type(column_type):
            available_types.extend(
                [
                    AnalysisType.COUNT,
                    AnalysisType.DATE_RANGE,
                    AnalysisType.YEAR_ANALYSIS,
                    AnalysisType.MONTH_ANALYSIS,
                    AnalysisType.DAY_ANALYSIS,
                ]
            )

        return available_types

    def generate_sql(
        self,
        column_name: str,
        analysis_type: AnalysisType,
        group_by_columns: Optional[List[str]] = None,
        where_conditions: Optional[Dict[str, Union[str, Tuple, List]]] = None,
        limit: Optional[int] = None,
        top_k: Optional[int] = 10,
    ) -> str:
        """
        生成SQL语句

        Args:
            column_name: 要分析的字段名
            analysis_type: 分析类型
            group_by_columns: 分组字段列表
            where_conditions: WHERE条件字典
            limit: 限制结果数量
            top_k: TOP-K分析时的K值

        Returns:
            生成的SQL语句
        """
        select_clause = self._build_select_clause(column_name, analysis_type)

        from_clause = f"FROM {self.table_name}"

        where_clause = self._build_where_clause(where_conditions)

        group_by_clause = self._build_group_by_clause(group_by_columns)

        order_by_clause = self._build_order_by_clause(column_name, analysis_type)

        limit_clause = self._build_limit_clause(limit, analysis_type, top_k)

        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if group_by_clause:
            sql_parts.append(group_by_clause)
        if order_by_clause:
            sql_parts.append(order_by_clause)
        if limit_clause:
            sql_parts.append(limit_clause)

        return " ".join(sql_parts)

    def _build_select_clause(
        self, column_name: str, analysis_type: AnalysisType
    ) -> str:
        """
        构建SELECT子句

        Args:
            column_name: 字段名
            analysis_type: 分析类型

        Returns:
            SELECT子句
        """
        if analysis_type not in self.SQL_TEMPLATES:
            raise ValueError(f"Unsupported analysis type: {analysis_type}")

        template = self.SQL_TEMPLATES[analysis_type]
        return template.format(column=column_name, table=self.table_name)

    def _build_where_clause(
        self, where_conditions: Optional[Dict[str, Union[str, Tuple, List]]]
    ) -> str:
        """
        构建WHERE子句

        Args:
            where_conditions: WHERE条件字典

        Returns:
            WHERE子句
        """
        if not where_conditions:
            return ""

        conditions = []
        for col, condition in where_conditions.items():
            if isinstance(condition, tuple) and len(condition) == 2:
                # 格式: ('>', 30) -> column > 30
                op, val = condition
                if isinstance(val, str):
                    val = f"'{val}'"
                conditions.append(f"{col} {op} {val}")
            elif isinstance(condition, str):
                # 格式: '> 30' -> column > 30
                conditions.append(f"{col} {condition}")
            else:
                # 格式: 30 -> column = 30
                if isinstance(condition, str):
                    condition = f"'{condition}'"
                conditions.append(f"{col} = {condition}")

        return f"WHERE {' AND '.join(conditions)}" if conditions else ""

    def _build_group_by_clause(self, group_by_columns: Optional[List[str]]) -> str:
        """
        构建GROUP BY子句

        Args:
            group_by_columns: 分组字段列表

        Returns:
            GROUP BY子句
        """
        if not group_by_columns:
            return ""
        return f"GROUP BY {', '.join(group_by_columns)}"

    def _build_order_by_clause(
        self, column_name: str, analysis_type: AnalysisType
    ) -> str:
        """
        构建ORDER BY子句

        Args:
            column_name: 字段名
            analysis_type: 分析类型

        Returns:
            ORDER BY子句
        """
        if analysis_type in [AnalysisType.TOP_K, AnalysisType.VALUE_DISTRIBUTION]:
            return f"ORDER BY count DESC"
        elif analysis_type in [
            AnalysisType.YEAR_ANALYSIS,
            AnalysisType.MONTH_ANALYSIS,
            AnalysisType.DAY_ANALYSIS,
        ]:
            return f"ORDER BY count DESC"
        return ""

    def _build_limit_clause(
        self, limit: Optional[int], analysis_type: AnalysisType, top_k: int
    ) -> str:
        """
        构建LIMIT子句

        Args:
            limit: 限制返回的记录数
            analysis_type: 分析类型
            top_k: 返回前K个值

        Returns:
            LIMIT子句
        """
        if analysis_type == AnalysisType.TOP_K:
            return f"LIMIT {top_k}"
        elif limit:
            return f"LIMIT {limit}"
        return ""

    def get_analysis_examples(self, column_name: str) -> List[Dict[str, str]]:
        """
        获取字段的分析示例

        Args:
            column_name: 字段名

        Returns:
            分析示例列表
        """
        available_types = self.get_available_analysis_types(column_name)
        examples = []

        for analysis_type in available_types:
            try:
                sql = self.generate_sql(column_name, analysis_type)
                examples.append(
                    {
                        "type": analysis_type.value,
                        "description": self._get_analysis_description(analysis_type),
                        "sql_example": sql,
                    }
                )
            except Exception as e:
                continue

        return examples

    def _get_analysis_description(self, analysis_type: AnalysisType) -> str:
        """
        获取分析类型的描述

        Args:
            analysis_type: 分析类型

        Returns:
            分析类型的描述
        """
        return self.ANALYSIS_DESCRIPTIONS.get(
            analysis_type, f"{analysis_type.value}分析"
        )


def generate_multi_column_sql(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    analysis_config: Dict[str, AnalysisType],
    group_by_columns: Optional[List[str]] = None,
    where_conditions: Optional[Dict[str, Union[str, Tuple, List]]] = None,
) -> str:
    """
    生成多字段分析的SQL语句

    Args:
        conn: DuckDB连接对象
        table_name: 表名
        analysis_config: 分析配置 {字段名: 分析类型}
        group_by_columns: 分组字段列表
        where_conditions: WHERE条件字典

    Returns:
        生成的SQL语句
    """
    generator = SQLGenerator(conn, table_name)

    select_parts = []
    for column_name, analysis_type in analysis_config.items():
        select_clause = generator._build_select_clause(column_name, analysis_type)
        select_parts.append(select_clause.replace("SELECT ", ""))

    select_clause = f"SELECT {', '.join(select_parts)}"

    from_clause = f"FROM {table_name}"
    where_clause = generator._build_where_clause(where_conditions)
    group_by_clause = generator._build_group_by_clause(group_by_columns)

    sql_parts = [select_clause, from_clause]
    if where_clause:
        sql_parts.append(where_clause)
    if group_by_clause:
        sql_parts.append(group_by_clause)

    return " ".join(sql_parts)
