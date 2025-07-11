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
        AnalysisType.MEDIAN: "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as median_{column}",
        AnalysisType.QUARTILES: "SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as q1_{column}, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as q2_{column}, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as q3_{column}",
        AnalysisType.PERCENTILES: "SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY {column}) as p10_{column}, PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as p25_{column}, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as p50_{column}, PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as p75_{column}, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {column}) as p90_{column}",
        AnalysisType.DISTINCT_COUNT: "SELECT COUNT(DISTINCT {column}) as distinct_count_{column}",
        AnalysisType.TOP_K: "SELECT {column}, COUNT(*) as count",
        AnalysisType.VALUE_DISTRIBUTION: "SELECT {column}, COUNT(*) as count",
        AnalysisType.LENGTH_ANALYSIS: "SELECT LENGTH({column}) as length, COUNT(*) as count",
        AnalysisType.PATTERN_ANALYSIS: "SELECT {column}, COUNT(*) as count",
        AnalysisType.DATE_RANGE: "SELECT MIN({column}) as min_date, MAX({column}) as max_date",
        AnalysisType.YEAR_ANALYSIS: "SELECT EXTRACT(YEAR FROM {column}) as year, COUNT(*) as count",
        AnalysisType.MONTH_ANALYSIS: "SELECT EXTRACT(MONTH FROM {column}) as month, COUNT(*) as count",
        AnalysisType.DAY_ANALYSIS: "SELECT EXTRACT(DAY FROM {column}) as day, COUNT(*) as count",
        AnalysisType.HOUR_ANALYSIS: "SELECT EXTRACT(HOUR FROM {column}) as hour, COUNT(*) as count",
        AnalysisType.WEEKDAY_ANALYSIS: "SELECT EXTRACT(DOW FROM {column}) as weekday, COUNT(*) as count",
        AnalysisType.SEASONAL_ANALYSIS: "SELECT CASE WHEN EXTRACT(MONTH FROM {column}) IN (12, 1, 2) THEN 'Winter' WHEN EXTRACT(MONTH FROM {column}) IN (3, 4, 5) THEN 'Spring' WHEN EXTRACT(MONTH FROM {column}) IN (6, 7, 8) THEN 'Summer' ELSE 'Fall' END as season, COUNT(*) as count",
        AnalysisType.MISSING_VALUES: "SELECT COUNT(*) as total_count, COUNT({column}) as non_null_count, COUNT(*) - COUNT({column}) as null_count",
        AnalysisType.DATA_QUALITY: "SELECT COUNT(*) as total_count, COUNT({column}) as non_null_count, COUNT(DISTINCT {column}) as distinct_count",
        AnalysisType.CORRELATION: "SELECT CORR({column}, {column}) as correlation",
    }

    # 分析类型描述映射
    ANALYSIS_DESCRIPTIONS = {
        AnalysisType.AVG: "计算平均值",
        AnalysisType.MAX: "获取最大值",
        AnalysisType.MIN: "获取最小值",
        AnalysisType.SUM: "计算总和",
        AnalysisType.VAR_POP: "计算总体方差",
        AnalysisType.STDDEV_POP: "计算总体标准差",
        AnalysisType.COUNT: "计算记录数",
        AnalysisType.MEDIAN: "计算中位数",
        AnalysisType.QUARTILES: "计算四分位数",
        AnalysisType.PERCENTILES: "计算百分位数",
        AnalysisType.DISTINCT_COUNT: "统计唯一值数量",
        AnalysisType.TOP_K: "获取前K个最常见值",
        AnalysisType.VALUE_DISTRIBUTION: "值分布统计",
        AnalysisType.LENGTH_ANALYSIS: "字符串长度分析",
        AnalysisType.PATTERN_ANALYSIS: "模式识别分析",
        AnalysisType.DATE_RANGE: "时间范围分析",
        AnalysisType.YEAR_ANALYSIS: "按年度分析",
        AnalysisType.MONTH_ANALYSIS: "按月度分析",
        AnalysisType.DAY_ANALYSIS: "按日期分析",
        AnalysisType.HOUR_ANALYSIS: "按小时分析",
        AnalysisType.WEEKDAY_ANALYSIS: "按星期分析",
        AnalysisType.SEASONAL_ANALYSIS: "季节性分析",
        AnalysisType.MISSING_VALUES: "缺失值分析",
        AnalysisType.DATA_QUALITY: "数据质量检查",
        AnalysisType.CORRELATION: "相关性分析",
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
                    AnalysisType.MEDIAN,
                    AnalysisType.QUARTILES,
                    AnalysisType.PERCENTILES,
                ]
            )
        elif is_duckdb_text_type(column_type):
            available_types.extend(
                [
                    AnalysisType.COUNT,
                    AnalysisType.DISTINCT_COUNT,
                    AnalysisType.TOP_K,
                    AnalysisType.VALUE_DISTRIBUTION,
                    AnalysisType.LENGTH_ANALYSIS,
                    AnalysisType.PATTERN_ANALYSIS,
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
                    AnalysisType.HOUR_ANALYSIS,
                    AnalysisType.WEEKDAY_ANALYSIS,
                    AnalysisType.SEASONAL_ANALYSIS,
                ]
            )
        else:
            # 通用分析类型
            available_types.extend(
                [
                    AnalysisType.COUNT,
                    AnalysisType.MISSING_VALUES,
                    AnalysisType.DATA_QUALITY,
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
        second_column: Optional[str] = None,
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
            second_column: 第二个列名（用于相关性分析）

        Returns:
            生成的SQL语句
        """
        if analysis_type == AnalysisType.CORRELATION and second_column:
            return self._generate_correlation_sql(
                column_name, second_column, where_conditions
            )

        if group_by_columns:
            base_select = self._build_select_clause(column_name, analysis_type)
            group_by_select = ", ".join(group_by_columns)
            if "SELECT" in base_select:
                select_part = base_select.replace("SELECT ", "")
                select_clause = f"SELECT {group_by_select}, {select_part}"
            else:
                select_clause = f"SELECT {group_by_select}, {base_select}"
        else:
            select_clause = self._build_select_clause(column_name, analysis_type)

        from_clause = f"FROM {self.table_name}"

        where_clause = self._build_where_clause(where_conditions)

        if analysis_type in [
            AnalysisType.TOP_K,
            AnalysisType.VALUE_DISTRIBUTION,
            AnalysisType.LENGTH_ANALYSIS,
            AnalysisType.PATTERN_ANALYSIS,
            AnalysisType.YEAR_ANALYSIS,
            AnalysisType.MONTH_ANALYSIS,
            AnalysisType.DAY_ANALYSIS,
            AnalysisType.HOUR_ANALYSIS,
            AnalysisType.WEEKDAY_ANALYSIS,
            AnalysisType.SEASONAL_ANALYSIS,
        ]:
            if where_clause:
                where_clause = f"{where_clause} AND {column_name} IS NOT NULL"
            else:
                where_clause = f"WHERE {column_name} IS NOT NULL"

        group_by_clause = ""
        if analysis_type in [
            AnalysisType.TOP_K,
            AnalysisType.VALUE_DISTRIBUTION,
            AnalysisType.LENGTH_ANALYSIS,
            AnalysisType.PATTERN_ANALYSIS,
            AnalysisType.YEAR_ANALYSIS,
            AnalysisType.MONTH_ANALYSIS,
            AnalysisType.DAY_ANALYSIS,
            AnalysisType.HOUR_ANALYSIS,
            AnalysisType.WEEKDAY_ANALYSIS,
            AnalysisType.SEASONAL_ANALYSIS,
        ]:
            if analysis_type == AnalysisType.LENGTH_ANALYSIS:
                group_by_clause = f"GROUP BY LENGTH({column_name})"
            elif analysis_type == AnalysisType.SEASONAL_ANALYSIS:
                group_by_clause = f"GROUP BY CASE WHEN EXTRACT(MONTH FROM {column_name}) IN (12, 1, 2) THEN 'Winter' WHEN EXTRACT(MONTH FROM {column_name}) IN (3, 4, 5) THEN 'Spring' WHEN EXTRACT(MONTH FROM {column_name}) IN (6, 7, 8) THEN 'Summer' ELSE 'Fall' END"
            elif analysis_type in [
                AnalysisType.YEAR_ANALYSIS,
                AnalysisType.MONTH_ANALYSIS,
                AnalysisType.DAY_ANALYSIS,
                AnalysisType.HOUR_ANALYSIS,
            ]:
                extract_part = analysis_type.value.replace("_analysis", "")
                group_by_clause = (
                    f"GROUP BY EXTRACT({extract_part.upper()} FROM {column_name})"
                )
            elif analysis_type == AnalysisType.WEEKDAY_ANALYSIS:
                group_by_clause = f"GROUP BY EXTRACT(DOW FROM {column_name})"
            else:
                group_by_clause = f"GROUP BY {column_name}"
        elif group_by_columns:
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

    def _generate_correlation_sql(
        self,
        column1: str,
        column2: str,
        where_conditions: Optional[Dict[str, Union[str, Tuple, List]]] = None,
    ) -> str:
        """
        生成相关性分析的SQL语句

        Args:
            column1: 第一个列名
            column2: 第二个列名
            where_conditions: WHERE条件字典

        Returns:
            相关性分析的SQL语句
        """
        select_clause = f"SELECT CORR({column1}, {column2}) as correlation"
        from_clause = f"FROM {self.table_name}"

        where_clause = self._build_where_clause(where_conditions)
        if where_clause:
            where_clause = (
                f"{where_clause} AND {column1} IS NOT NULL AND {column2} IS NOT NULL"
            )
        else:
            where_clause = f"WHERE {column1} IS NOT NULL AND {column2} IS NOT NULL"

        return f"{select_clause} {from_clause} {where_clause}"

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
        if analysis_type in [
            AnalysisType.TOP_K,
            AnalysisType.VALUE_DISTRIBUTION,
            AnalysisType.LENGTH_ANALYSIS,
            AnalysisType.PATTERN_ANALYSIS,
        ]:
            return f"ORDER BY count DESC"
        elif analysis_type in [
            AnalysisType.YEAR_ANALYSIS,
            AnalysisType.MONTH_ANALYSIS,
            AnalysisType.DAY_ANALYSIS,
            AnalysisType.HOUR_ANALYSIS,
            AnalysisType.WEEKDAY_ANALYSIS,
            AnalysisType.SEASONAL_ANALYSIS,
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
        if limit is not None:
            return f"LIMIT {limit}"
        elif analysis_type == AnalysisType.TOP_K:
            return f"LIMIT {top_k}"
        elif analysis_type in [
            AnalysisType.VALUE_DISTRIBUTION,
            AnalysisType.LENGTH_ANALYSIS,
            AnalysisType.PATTERN_ANALYSIS,
        ]:
            return f"LIMIT {top_k}"
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
                        "sql": sql,
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

    if group_by_columns:
        group_by_select = ", ".join(group_by_columns)
        select_clause = f"SELECT {group_by_select}, {', '.join(select_parts)}"
    else:
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
