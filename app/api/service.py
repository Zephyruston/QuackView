import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..analyzer.excel_analyzer import ExcelAnalyzer
from ..connector.excel_connector import AnalysisMode, ExcelConnector

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

        clean_filename = Path(filename).name
        logger.info(f"[Service] 清洗后的文件名: {clean_filename}")

        file_path = self.temp_dir / f"{task_id}_{clean_filename}"
        with open(file_path, "wb") as f:
            f.write(file_content)
        logger.info(f"[Service] 文件已保存到: {file_path}")

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
            raise

        try:
            schema_info = analyzer.import_and_analyze(str(file_path))
            logger.info(
                f"[Service] 数据结构分析成功, 表名: {schema_info.get('table_info',{}).get('table_name')}"
            )
        except Exception as e:
            logger.error(f"[Service] 数据结构分析失败: {e}", exc_info=True)
            raise

        try:
            con = connector.connect()
            logger.info(f"[Service] DuckDB连接获取成功")
        except Exception as e:
            logger.error(f"[Service] DuckDB连接获取失败: {e}", exc_info=True)
            raise

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
            raise ValueError(f"Session {task_id} not found")

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
            raise ValueError(f"Session {task_id} not found")

        session = self.sessions[task_id]
        schema_info = session["schema"]

        analysis_options = schema_info.get("analysis_options", {})

        options = []
        for column_name, analyses in analysis_options.items():
            operations = [analysis["type"].upper() for analysis in analyses]
            options.append({"column": column_name, "operations": operations})

        return options

    def get_session_info(self, task_id: str) -> Dict[str, Any]:
        """获取会话信息，包括表名"""
        if task_id not in self.sessions:
            raise ValueError(f"Session {task_id} not found")

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
        self, task_id: str, operations: List[Dict], filters: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """执行分析任务"""
        if task_id not in self.sessions:
            raise ValueError(f"Session {task_id} not found")

        session = self.sessions[task_id]
        con = session["connection"]
        table_name = session["table_name"]

        select_parts = []
        for op in operations:
            column = op["column"]
            operation = op["operation"]
            if operation == "SUM":
                select_parts.append(f"SUM({column}) as sum_{column}")
            elif operation == "AVG":
                select_parts.append(f"AVG({column}) as avg_{column}")
            elif operation == "MAX":
                select_parts.append(f"MAX({column}) as max_{column}")
            elif operation == "MIN":
                select_parts.append(f"MIN({column}) as min_{column}")
            elif operation == "COUNT":
                select_parts.append(f"COUNT({column}) as count_{column}")
            elif operation == "COUNT_DISTINCT":
                select_parts.append(
                    f"COUNT(DISTINCT {column}) as distinct_count_{column}"
                )

        sql = f"SELECT {', '.join(select_parts)} FROM {table_name}"

        if filters:
            where_conditions = []
            for f in filters:
                column = f["column"]
                operator = f["operator"]
                value = f["value"]

                if operator == "=":
                    where_conditions.append(f"{column} = '{value}'")
                elif operator == "BETWEEN":
                    where_conditions.append(
                        f"{column} BETWEEN '{value[0]}' AND '{value[1]}'"
                    )
                elif operator == ">":
                    where_conditions.append(f"{column} > {value}")
                elif operator == "<":
                    where_conditions.append(f"{column} < {value}")

            if where_conditions:
                sql += f" WHERE {' AND '.join(where_conditions)}"

        try:
            result_df = con.execute(sql).df()
            columns = result_df.columns.tolist()
            rows = result_df.values.tolist()

            return {"columns": columns, "rows": rows, "sql_preview": sql}
        except Exception as e:
            raise ValueError(f"SQL执行错误: {str(e)}")

    def execute_custom_query(self, task_id: str, sql: str) -> Dict[str, Any]:
        """执行自定义SQL查询"""
        if task_id not in self.sessions:
            raise ValueError(f"Session {task_id} not found")

        session = self.sessions[task_id]
        con = session["connection"]
        table_name = session["table_name"]

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
            if "Session" in str(e) and "not found" in str(e):
                raise ValueError(f"Session {task_id} not found")
            else:
                raise ValueError(f"SQL执行错误: {str(e)}")

    def export_sql(self, task_id: str) -> str:
        """导出SQL脚本"""
        if task_id not in self.sessions:
            raise ValueError(f"Session {task_id} not found")

        session = self.sessions[task_id]
        table_name = session["table_name"]

        sql = f"-- QuackView SQL Export\n"
        sql += f"-- Table: {table_name}\n\n"
        sql += f"SELECT * FROM {table_name};\n"

        return sql

    def export_excel(self, task_id: str) -> bytes:
        """导出Excel文件"""
        if task_id not in self.sessions:
            raise ValueError(f"Session {task_id} not found")

        session = self.sessions[task_id]
        con = session["connection"]
        table_name = session["table_name"]

        df = con.execute(f"SELECT * FROM {table_name}").df()

        output_path = self.temp_dir / f"export_{task_id}.xlsx"
        df.to_excel(output_path, index=False)

        with open(output_path, "rb") as f:
            return f.read()

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
