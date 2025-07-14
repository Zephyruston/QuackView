import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..analyzer.excel_analyzer import ExcelAnalyzer
from ..connector.excel_connector import AnalysisMode, ExcelConnector
from .exceptions import (
    AnalysisError,
    DatabaseConnectionError,
    ExcelProcessingError,
    ExportError,
    FileValidationError,
    SessionNotFoundError,
    SQLExecutionError,
)

logger = logging.getLogger(__name__)


class QuackViewService:
    """QuackView API服务层"""

    def __init__(self):
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.temp_dir = Path(tempfile.gettempdir()) / "quackview"
        self.temp_dir.mkdir(exist_ok=True)

    def create_session(self, file_content: bytes, filename: str) -> str:
        """创建新的分析会话"""
        task_id = str(uuid.uuid4())
        logger.info(f"[Service] 创建新会话: {task_id}, 文件名: {filename}")

        if not file_content or len(file_content) == 0:
            raise FileValidationError("File content is empty", filename)

        clean_filename = Path(filename).name
        logger.info(f"[Service] 清洗后的文件名: {clean_filename}")

        file_path = self.temp_dir / f"{task_id}_{clean_filename}"
        try:
            with open(file_path, "wb") as f:
                f.write(file_content)
            logger.info(f"[Service] 文件已保存到: {file_path}")
        except Exception as e:
            logger.error(f"[Service] 文件保存失败: {e}", exc_info=True)
            raise FileValidationError(f"Failed to save file: {str(e)}", filename)

        connector = ExcelConnector(mode=AnalysisMode.MEMORY)
        analyzer = ExcelAnalyzer(mode=AnalysisMode.MEMORY)
        logger.info(f"[Service] 初始化连接器和分析器 (内存模式)")

        try:
            df = connector.import_excel(str(file_path))
            logger.info(
                f"[Service] Excel导入成功, shape={df.shape if hasattr(df, 'shape') else 'unknown'}"
            )
        except Exception as e:
            logger.error(f"[Service] Excel导入失败: {e}", exc_info=True)
            raise ExcelProcessingError(
                f"Failed to import Excel file: {str(e)}", filename
            )

        try:
            schema_info = analyzer.import_and_analyze(str(file_path))
            logger.info(
                f"[Service] 数据结构分析成功, 表名: {schema_info.get('table_info',{}).get('table_name')}"
            )
        except Exception as e:
            logger.error(f"[Service] 数据结构分析失败: {e}", exc_info=True)
            raise ExcelProcessingError(
                f"Failed to analyze Excel structure: {str(e)}", filename
            )

        try:
            con = connector.connect()
            logger.info(f"[Service] DuckDB连接获取成功")
        except Exception as e:
            logger.error(f"[Service] DuckDB连接获取失败: {e}", exc_info=True)
            raise DatabaseConnectionError(f"Failed to connect to database: {str(e)}")

        table_info = schema_info.get("table_info", {})
        table_name = table_info.get("table_name", "data")

        if table_name and not table_name.startswith("tbl_"):
            table_name = "tbl_" + table_name
        elif not table_name:
            table_name = "tbl_data"

        self.sessions[task_id] = {
            "file_path": str(file_path),
            "table_name": table_name,
            "schema": schema_info,
            "connection": con,
            "connector": connector,
            "analyzer": analyzer,
            "created_at": time.time(),
        }
        logger.info(f"[Service] 会话信息已保存: {task_id}, 表名: {table_name}")

        return task_id

    def get_schema(self, task_id: str) -> Dict[str, Any]:
        """获取表结构信息"""
        if task_id not in self.sessions:
            raise SessionNotFoundError(task_id)

        session = self.sessions[task_id]
        schema_info = session["schema"]

        table_info = schema_info.get("table_info", {})

        return {
            "table_name": session["table_name"],
            "columns": [
                {"name": col["name"], "type": col["type"]}
                for col in table_info.get("columns", [])
            ],
        }

    def get_analysis_options(self, task_id: str) -> List[Dict[str, Any]]:
        """获取分析选项"""
        if task_id not in self.sessions:
            raise SessionNotFoundError(task_id)

        session = self.sessions[task_id]
        con = session["connection"]
        table_name = session["table_name"]
        schema_info = session["schema"]

        try:
            from ..generator.sql_generator import SQLGenerator

            generator = SQLGenerator(con, table_name)

            options = []
            table_info = schema_info.get("table_info", {})

            for column_info in table_info.get("columns", []):
                column_name = column_info["name"]
                available_types = generator.get_available_analysis_types(column_name)

                operations = [
                    analysis_type.value.upper() for analysis_type in available_types
                ]

                options.append(
                    {
                        "column": column_name,
                        "operations": operations,
                        "type": column_info["type"],
                    }
                )

            return options

        except Exception as e:
            logger.error(f"[Service] 获取分析选项失败: {str(e)}")
            analysis_options = schema_info.get("analysis_options", {})
            options = []
            for column_name, analyses in analysis_options.items():
                operations = [analysis["type"].upper() for analysis in analyses]
                options.append({"column": column_name, "operations": operations})
            return options

    def get_session_info(self, task_id: str) -> Dict[str, Any]:
        """获取会话信息，包括表名"""
        if task_id not in self.sessions:
            raise SessionNotFoundError(task_id)

        session = self.sessions[task_id]

        return {
            "task_id": task_id,
            "table_name": session["table_name"],
            "file_path": session["file_path"],
            "created_at": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(session["created_at"])
            ),
        }

    def execute_analysis(
        self,
        task_id: str,
        operations: List[Dict],
        filters: Optional[List[Dict]] = None,
        group_by: Optional[List[str]] = None,
        sort_by: Optional[List[Dict]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """执行分析任务"""
        logger.info(
            f"[Service] 开始执行分析: task_id={task_id}, operations={operations}, filters={filters}, group_by={group_by}, sort_by={sort_by}"
        )

        if task_id not in self.sessions:
            raise SessionNotFoundError(task_id)

        session = self.sessions[task_id]
        con = session["connection"]
        table_name = session["table_name"]

        if not operations:
            raise AnalysisError("No analysis operations provided", task_id)

        try:
            from ..generator.sql_generator import AnalysisType, SQLGenerator

            generator = SQLGenerator(con, table_name)

            converted_operations = []
            for op in operations:
                column = op["column"]
                operation = op["operation"]

                operation_mapping = {
                    "SUM": AnalysisType.SUM,
                    "AVG": AnalysisType.AVG,
                    "MAX": AnalysisType.MAX,
                    "MIN": AnalysisType.MIN,
                    "COUNT": AnalysisType.COUNT,
                    "COUNT_DISTINCT": AnalysisType.DISTINCT_COUNT,
                    "VAR_POP": AnalysisType.VAR_POP,
                    "STDDEV_POP": AnalysisType.STDDEV_POP,
                    "MEDIAN": AnalysisType.MEDIAN,
                    "QUARTILES": AnalysisType.QUARTILES,
                    "PERCENTILES": AnalysisType.PERCENTILES,
                    "TOP_K": AnalysisType.TOP_K,
                    "VALUE_DISTRIBUTION": AnalysisType.VALUE_DISTRIBUTION,
                    "LENGTH_ANALYSIS": AnalysisType.LENGTH_ANALYSIS,
                    "PATTERN_ANALYSIS": AnalysisType.PATTERN_ANALYSIS,
                    "DATE_RANGE": AnalysisType.DATE_RANGE,
                    "YEAR_ANALYSIS": AnalysisType.YEAR_ANALYSIS,
                    "MONTH_ANALYSIS": AnalysisType.MONTH_ANALYSIS,
                    "DAY_ANALYSIS": AnalysisType.DAY_ANALYSIS,
                    "HOUR_ANALYSIS": AnalysisType.HOUR_ANALYSIS,
                    "WEEKDAY_ANALYSIS": AnalysisType.WEEKDAY_ANALYSIS,
                    "SEASONAL_ANALYSIS": AnalysisType.SEASONAL_ANALYSIS,
                    "MISSING_VALUES": AnalysisType.MISSING_VALUES,
                    "DATA_QUALITY": AnalysisType.DATA_QUALITY,
                    "CORRELATION": AnalysisType.CORRELATION,
                    "SELECT": AnalysisType.SELECT,
                }

                if operation not in operation_mapping:
                    raise AnalysisError(f"Unsupported operation: {operation}", task_id)

                converted_operations.append(
                    {"column": column, "analysis_type": operation_mapping[operation]}
                )

            where_conditions = None
            if filters:
                where_conditions = {}
                for f in filters:
                    column = f["column"]
                    operator = f["operator"]
                    value = f["value"]

                    if operator == "=":
                        where_conditions[column] = ("=", value)
                    elif operator == "BETWEEN":
                        where_conditions[column] = ("BETWEEN", value)
                    elif operator == ">":
                        where_conditions[column] = (">", value)
                    elif operator == "<":
                        where_conditions[column] = ("<", value)
                    elif operator == ">=":
                        where_conditions[column] = (">=", value)
                    elif operator == "<=":
                        where_conditions[column] = ("<=", value)
                    elif operator == "!=":
                        where_conditions[column] = ("!=", value)
                    elif operator == "LIKE":
                        where_conditions[column] = ("LIKE", value)
                    else:
                        raise AnalysisError(
                            f"Unsupported operator: {operator}", task_id
                        )

            if len(converted_operations) == 1:
                op = converted_operations[0]
                sql = generator.generate_sql(
                    column_name=op["column"],
                    analysis_type=op["analysis_type"],
                    group_by_columns=group_by,
                    where_conditions=where_conditions,
                    sort_by=sort_by,
                    limit=limit,
                )
            else:
                if len(set(op["column"] for op in converted_operations)) == 1:
                    select_parts = []
                    for op in converted_operations:
                        select_clause = generator._build_select_clause(
                            op["column"], op["analysis_type"]
                        )
                        select_parts.append(select_clause.replace("SELECT ", ""))

                    if group_by:
                        group_by_select = ", ".join(group_by)
                        select_clause = (
                            f"SELECT {group_by_select}, {', '.join(select_parts)}"
                        )
                    else:
                        select_clause = f"SELECT {', '.join(select_parts)}"

                    from_clause = f"FROM {table_name}"
                    where_clause = generator._build_where_clause(where_conditions)
                    group_by_clause = generator._build_group_by_clause(group_by)
                    order_by_clause = generator._build_custom_order_by_clause(sort_by)

                    sql_parts = [select_clause, from_clause]
                    if where_clause:
                        sql_parts.append(where_clause)
                    if group_by_clause:
                        sql_parts.append(group_by_clause)
                    if order_by_clause:
                        sql_parts.append(order_by_clause)

                    sql = " ".join(sql_parts)
                else:
                    select_parts = []
                    for op in converted_operations:
                        select_clause = generator._build_select_clause(
                            op["column"], op["analysis_type"]
                        )
                        select_parts.append(select_clause.replace("SELECT ", ""))

                    if group_by:
                        group_by_select = ", ".join(group_by)
                        select_clause = (
                            f"SELECT {group_by_select}, {', '.join(select_parts)}"
                        )
                    else:
                        select_clause = f"SELECT {', '.join(select_parts)}"

                    from_clause = f"FROM {table_name}"
                    where_clause = generator._build_where_clause(where_conditions)
                    group_by_clause = generator._build_group_by_clause(group_by)
                    order_by_clause = generator._build_custom_order_by_clause(sort_by)

                    sql_parts = [select_clause, from_clause]
                    if where_clause:
                        sql_parts.append(where_clause)
                    if group_by_clause:
                        sql_parts.append(group_by_clause)
                    if order_by_clause:
                        sql_parts.append(order_by_clause)

                    sql = " ".join(sql_parts)

            logger.info(f"[Service] 生成的SQL: {sql}")

            result_df = con.execute(sql).df()
            columns = result_df.columns.tolist()
            rows = result_df.values.tolist()

            logger.info(
                f"[Service] 分析执行成功: 列数={len(columns)}, 行数={len(rows)}"
            )
            return {"columns": columns, "rows": rows, "sql_preview": sql}

        except Exception as e:
            logger.error(f"[Service] SQL执行失败: {str(e)}")
            raise SQLExecutionError(f"SQL execution failed: {str(e)}", "")

    def execute_custom_query(self, task_id: str, sql: str) -> Dict[str, Any]:
        """执行自定义SQL查询"""
        if task_id not in self.sessions:
            raise SessionNotFoundError(task_id)

        session = self.sessions[task_id]
        con = session["connection"]
        table_name = session["table_name"]

        if not sql or not sql.strip():
            raise SQLExecutionError("SQL query is empty", sql)

        processed_sql = sql
        common_table_names = ["data", "DATA", "table", "TABLE", "main", "MAIN"]

        for common_name in common_table_names:
            processed_sql = processed_sql.replace(
                f"FROM {common_name}", f"FROM {table_name}"
            )
            processed_sql = processed_sql.replace(
                f"from {common_name}", f"from {table_name}"
            )
            processed_sql = processed_sql.replace(
                f"JOIN {common_name}", f"JOIN {table_name}"
            )
            processed_sql = processed_sql.replace(
                f"join {common_name}", f"join {table_name}"
            )
            processed_sql = processed_sql.replace(
                f"UPDATE {common_name}", f"UPDATE {table_name}"
            )
            processed_sql = processed_sql.replace(
                f"update {common_name}", f"update {table_name}"
            )
            processed_sql = processed_sql.replace(
                f"DELETE FROM {common_name}", f"DELETE FROM {table_name}"
            )
            processed_sql = processed_sql.replace(
                f"delete from {common_name}", f"delete from {table_name}"
            )

        try:
            result_df = con.execute(processed_sql).df()
            columns = result_df.columns.tolist()
            rows = result_df.values.tolist()

            return {"columns": columns, "rows": rows, "sql_preview": processed_sql}
        except Exception as e:
            raise SQLExecutionError(f"SQL execution failed: {str(e)}", processed_sql)

    def export_sql(self, task_id: str) -> str:
        """导出SQL脚本"""
        if task_id not in self.sessions:
            raise SessionNotFoundError(task_id)

        session = self.sessions[task_id]
        table_name = session["table_name"]

        sql = f"-- QuackView SQL Export\n"
        sql += f"-- Table: {table_name}\n\n"
        sql += f"SELECT * FROM {table_name};\n"

        return sql

    def export_excel(self, task_id: str) -> bytes:
        """导出Excel文件"""
        if task_id not in self.sessions:
            raise SessionNotFoundError(task_id)

        session = self.sessions[task_id]
        con = session["connection"]
        table_name = session["table_name"]

        try:
            df = con.execute(f"SELECT * FROM {table_name}").df()
        except Exception as e:
            raise SQLExecutionError(
                f"Failed to query data for export: {str(e)}",
                f"SELECT * FROM {table_name}",
            )

        output_path = self.temp_dir / f"export_{task_id}.xlsx"
        try:
            df.to_excel(output_path, index=False)
        except Exception as e:
            raise ExportError(f"Failed to export Excel file: {str(e)}", "excel")

        try:
            with open(output_path, "rb") as f:
                return f.read()
        except Exception as e:
            raise ExportError(f"Failed to read exported file: {str(e)}", "excel")

    def close_session(self, task_id: str) -> bool:
        """关闭会话"""
        if task_id not in self.sessions:
            logger.warning(f"[Service] 关闭会话失败, 不存在: {task_id}")
            return False

        session = self.sessions[task_id]

        if "connection" in session:
            try:
                session["connection"].close()
                logger.info(f"[Service] DuckDB连接已关闭: {task_id}")
            except Exception as e:
                logger.warning(f"[Service] 关闭连接时出错: {e}")

        if "file_path" in session:
            try:
                os.remove(session["file_path"])
                logger.info(f"[Service] 临时文件已删除: {session['file_path']}")
            except Exception as e:
                logger.warning(f"[Service] 删除临时文件失败: {e}")

        del self.sessions[task_id]
        logger.info(f"[Service] 会话已删除: {task_id}")

        return True


service = QuackViewService()
