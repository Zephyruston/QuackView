import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class SQLExecutor:
    """SQL执行器类"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """
        初始化SQL执行器

        Args:
            conn: DuckDB连接对象
        """
        self.conn = conn

    def execute(self, sql: str) -> pd.DataFrame:
        """
        执行SQL查询并返回DataFrame结果

        Args:
            sql: SQL查询语句

        Returns:
            查询结果DataFrame
        """
        try:
            logger.info(f"执行SQL: {sql}")
            result = self.conn.execute(sql).fetch_df()
            logger.info(f"查询成功，返回 {len(result)} 行数据")
            return result
        except Exception as e:
            logger.error(f"SQL执行错误: {e}\nSQL: {sql}")
            raise

    def explain(self, sql: str) -> str:
        """
        返回SQL执行计划

        Args:
            sql: SQL查询语句

        Returns:
            执行计划字符串
        """
        try:
            result = self.conn.execute(f"EXPLAIN {sql}").fetchone()
            return result[0] if result else ""
        except Exception as e:
            logger.error(f"获取执行计划失败: {e}")
            return f"无法获取执行计划: {e}"

    def execute_with_plan(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL并返回结果和执行计划

        Args:
            sql: SQL查询语句

        Returns:
            包含结果和执行计划的字典
        """
        try:
            plan = self.explain(sql)

            result = self.execute(sql)

            return {
                "success": True,
                "result": result,
                "plan": plan,
                "sql": sql,
                "error": None,
            }
        except Exception as e:
            return {
                "success": False,
                "result": None,
                "plan": None,
                "sql": sql,
                "error": str(e),
            }

    def execute_analysis(
        self,
        table_name: str,
        column_name: str,
        analysis_type: str,
        group_by_columns: Optional[List[str]] = None,
        where_conditions: Optional[Dict[str, Union[str, Tuple, List]]] = None,
        limit: Optional[int] = None,
        top_k: Optional[int] = 10,
        second_column: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        执行分析查询

        Args:
            table_name: 表名
            column_name: 字段名
            analysis_type: 分析类型
            group_by_columns: 分组字段列表
            where_conditions: WHERE条件字典
            limit: 限制结果数量
            top_k: TOP-K分析时的K值
            second_column: 第二个列名（用于相关性分析）

        Returns:
            分析结果字典
        """
        try:
            from ..generator.sql_generator import AnalysisType, SQLGenerator

            generator = SQLGenerator(self.conn, table_name)

            try:
                analysis_enum = AnalysisType(analysis_type)
            except ValueError:
                return {
                    "success": False,
                    "result": None,
                    "sql": None,
                    "error": f"不支持的分析类型: {analysis_type}",
                }

            sql = generator.generate_sql(
                column_name=column_name,
                analysis_type=analysis_enum,
                group_by_columns=group_by_columns,
                where_conditions=where_conditions,
                limit=limit,
                top_k=top_k,
                second_column=second_column,
            )

            result = self.execute(sql)

            return {"success": True, "result": result, "sql": sql, "error": None}

        except Exception as e:
            return {"success": False, "result": None, "sql": None, "error": str(e)}

    def execute_multi_column_analysis(
        self,
        table_name: str,
        analysis_config: Dict[str, str],
        group_by_columns: Optional[List[str]] = None,
        where_conditions: Optional[Dict[str, Union[str, Tuple, List]]] = None,
    ) -> Dict[str, Any]:
        """
        执行多字段分析

        Args:
            table_name: 表名
            analysis_config: 分析配置 {字段名: 分析类型字符串}
            group_by_columns: 分组字段列表
            where_conditions: WHERE条件字典

        Returns:
            分析结果字典
        """
        try:
            from ..generator.sql_generator import (
                AnalysisType,
                generate_multi_column_sql,
            )

            enum_config = {}
            for column_name, analysis_type_str in analysis_config.items():
                try:
                    enum_config[column_name] = AnalysisType(analysis_type_str)
                except ValueError:
                    return {
                        "success": False,
                        "result": None,
                        "sql": None,
                        "error": f"不支持的分析类型: {analysis_type_str}",
                    }

            sql = generate_multi_column_sql(
                conn=self.conn,
                table_name=table_name,
                analysis_config=enum_config,
                group_by_columns=group_by_columns,
                where_conditions=where_conditions,
            )

            result = self.execute(sql)

            return {"success": True, "result": result, "sql": sql, "error": None}

        except Exception as e:
            return {"success": False, "result": None, "sql": None, "error": str(e)}

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        获取表结构

        Args:
            table_name: 表名

        Returns:
            列名到类型的映射字典
        """
        try:
            describe_result = self.conn.execute(f"DESCRIBE {table_name}").fetch_df()
            type_map = dict(
                zip(
                    describe_result["column_name"],
                    describe_result["column_type"].str.lower(),
                )
            )
            return type_map
        except Exception as e:
            logger.error(f"获取表结构失败: {e}")
            return {}

    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """
        获取表样本数据

        Args:
            table_name: 表名
            limit: 限制返回行数

        Returns:
            样本数据DataFrame
        """
        try:
            return self.conn.execute(
                f"SELECT * FROM {table_name} LIMIT {limit}"
            ).fetch_df()
        except Exception as e:
            logger.error(f"获取样本数据失败: {e}")
            return pd.DataFrame()

    def test_connection(self) -> bool:
        """
        测试数据库连接

        Returns:
            连接是否正常
        """
        try:
            self.conn.execute("SELECT 1").fetchone()
            return True
        except Exception as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False
