import unittest

import duckdb
import pandas as pd

from app.executor.sql_executor import SQLExecutor


class TestSQLExecutor(unittest.TestCase):
    """测试SQL执行器"""

    def setUp(self):
        """在每个测试前创建测试数据库和表"""
        self.conn = duckdb.connect(":memory:")

        # 创建测试表
        self.conn.execute(
            """
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR,
                score FLOAT,
                category VARCHAR,
                amount INTEGER,
                timestamp TIMESTAMP,
                rating DOUBLE
            )
        """
        )

        # 插入测试数据
        self.conn.execute(
            """
            INSERT INTO test_table VALUES 
            (1, 'Alice', 85.5, 'A', 100, '2023-01-01 10:00:00', 4.5),
            (2, 'Bob', 92.0, 'B', 200, '2023-01-02 11:00:00', 4.8),
            (3, 'Charlie', 78.5, 'A', 150, '2023-01-03 12:00:00', 4.2),
            (4, 'David', 88.0, 'C', 300, '2023-02-01 13:00:00', 4.6),
            (5, 'Eve', 95.5, 'B', 250, '2023-02-02 14:00:00', 4.9),
            (6, 'Frank', 82.0, 'A', 180, '2023-03-01 15:00:00', 4.3)
        """
        )

        self.executor = SQLExecutor(self.conn)

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_execute_simple_query(self):
        """测试执行简单查询"""
        result = self.executor.execute("SELECT COUNT(*) FROM test_table")
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0, 0], 6)

    def test_execute_select_query(self):
        """测试执行SELECT查询"""
        result = self.executor.execute(
            "SELECT name, score FROM test_table WHERE score > 80"
        )
        self.assertEqual(len(result), 5)  # 5个分数大于80的记录
        self.assertIn("name", result.columns)
        self.assertIn("score", result.columns)

    def test_execute_aggregation_query(self):
        """测试执行聚合查询"""
        result = self.executor.execute("SELECT AVG(score) as avg_score FROM test_table")
        self.assertEqual(len(result), 1)
        self.assertIn("avg_score", result.columns)
        self.assertGreater(result.iloc[0, 0], 0)

    def test_execute_with_error(self):
        """测试执行错误SQL"""
        with self.assertRaises(Exception):
            self.executor.execute("SELECT * FROM nonexistent_table")

    def test_explain_simple_query(self):
        """测试获取执行计划"""
        plan = self.executor.explain("SELECT * FROM test_table")
        self.assertIsInstance(plan, str)
        self.assertGreater(len(plan), 0)

    def test_explain_complex_query(self):
        """测试获取复杂查询的执行计划"""
        plan = self.executor.explain(
            """
            SELECT category, AVG(score) as avg_score 
            FROM test_table 
            WHERE score > 80 
            GROUP BY category 
            ORDER BY avg_score DESC
        """
        )
        self.assertIsInstance(plan, str)
        self.assertGreater(len(plan), 0)

    def test_explain_with_error(self):
        """测试获取错误SQL的执行计划"""
        plan = self.executor.explain("SELECT * FROM nonexistent_table")
        self.assertIn("无法获取执行计划", plan)

    def test_execute_with_plan_success(self):
        """测试成功执行带计划的查询"""
        result = self.executor.execute_with_plan("SELECT COUNT(*) FROM test_table")

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)
        self.assertIsInstance(result["plan"], str)
        self.assertEqual(result["sql"], "SELECT COUNT(*) FROM test_table")
        self.assertIsNone(result["error"])

    def test_execute_with_plan_error(self):
        """测试失败执行带计划的查询"""
        result = self.executor.execute_with_plan("SELECT * FROM nonexistent_table")

        self.assertFalse(result["success"])
        self.assertIsNone(result["result"])
        self.assertIsNone(result["plan"])
        self.assertEqual(result["sql"], "SELECT * FROM nonexistent_table")
        self.assertIsNotNone(result["error"])

    def test_execute_analysis_basic(self):
        """测试执行基本分析"""
        result = self.executor.execute_analysis("test_table", "score", "avg")

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)
        self.assertIsInstance(result["sql"], str)
        self.assertIsNone(result["error"])

    def test_execute_analysis_with_group_by(self):
        """测试执行带分组的分析"""
        result = self.executor.execute_analysis(
            "test_table", "score", "avg", group_by_columns=["category"]
        )

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)
        self.assertIn("category", result["result"].columns)

    def test_execute_analysis_with_where(self):
        """测试执行带条件的分析"""
        where_conditions = {"score": (">", 80)}
        result = self.executor.execute_analysis(
            "test_table", "score", "avg", where_conditions=where_conditions
        )

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)

    def test_execute_analysis_invalid_type(self):
        """测试执行无效分析类型"""
        result = self.executor.execute_analysis(
            "test_table", "score", "invalid_analysis_type"
        )

        self.assertFalse(result["success"])
        self.assertIsNone(result["result"])
        self.assertIsNone(result["sql"])
        self.assertIn("不支持的分析类型", result["error"])

    def test_execute_analysis_text_analysis(self):
        """测试执行文本分析"""
        result = self.executor.execute_analysis("test_table", "name", "distinct_count")

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)

    def test_execute_analysis_time_analysis(self):
        """测试执行时间分析"""
        result = self.executor.execute_analysis("test_table", "timestamp", "date_range")

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)

    def test_execute_multi_column_analysis(self):
        """测试执行多列分析"""
        analysis_config = {"score": "avg", "amount": "sum", "name": "count"}

        result = self.executor.execute_multi_column_analysis(
            "test_table", analysis_config
        )

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)
        self.assertIsInstance(result["sql"], str)

    def test_execute_multi_column_analysis_with_group_by(self):
        """测试执行带分组的多列分析"""
        analysis_config = {"score": "avg", "amount": "sum"}

        result = self.executor.execute_multi_column_analysis(
            "test_table", analysis_config, group_by_columns=["category"]
        )

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)
        self.assertIn("category", result["result"].columns)

    def test_execute_multi_column_analysis_invalid_type(self):
        """测试执行无效多列分析类型"""
        analysis_config = {"score": "avg", "amount": "invalid_type"}

        result = self.executor.execute_multi_column_analysis(
            "test_table", analysis_config
        )

        self.assertFalse(result["success"])
        self.assertIsNone(result["result"])
        self.assertIsNone(result["sql"])
        self.assertIn("不支持的分析类型", result["error"])

    def test_get_table_schema(self):
        """测试获取表结构"""
        schema = self.executor.get_table_schema("test_table")

        expected_columns = {
            "id",
            "name",
            "score",
            "category",
            "amount",
            "timestamp",
            "rating",
        }
        self.assertEqual(set(schema.keys()), expected_columns)

        # 检查类型映射
        self.assertEqual(schema["id"], "integer")
        self.assertEqual(schema["name"], "varchar")
        self.assertEqual(schema["score"], "float")

    def test_get_table_schema_nonexistent_table(self):
        """测试获取不存在表的结构"""
        schema = self.executor.get_table_schema("nonexistent_table")
        self.assertEqual(schema, {})

    def test_get_sample_data(self):
        """测试获取样本数据"""
        sample_data = self.executor.get_sample_data("test_table", limit=3)

        self.assertIsInstance(sample_data, pd.DataFrame)
        self.assertLessEqual(len(sample_data), 3)
        self.assertIn("id", sample_data.columns)
        self.assertIn("name", sample_data.columns)

    def test_get_sample_data_nonexistent_table(self):
        """测试获取不存在表的样本数据"""
        sample_data = self.executor.get_sample_data("nonexistent_table")
        self.assertTrue(sample_data.empty)

    def test_test_connection(self):
        """测试连接测试"""
        result = self.executor.test_connection()
        self.assertTrue(result)

    def test_execute_analysis_all_types(self):
        """测试所有分析类型的执行"""
        # 数值分析
        numeric_analyses = [
            "avg",
            "max",
            "min",
            "sum",
            "var_pop",
            "stddev_pop",
            "count",
            "median",
        ]
        for analysis_type in numeric_analyses:
            result = self.executor.execute_analysis(
                "test_table", "score", analysis_type
            )
            self.assertTrue(result["success"], f"Failed for {analysis_type}")

        # 文本分析
        text_analyses = ["count", "distinct_count", "top_k", "value_distribution"]
        for analysis_type in text_analyses:
            result = self.executor.execute_analysis("test_table", "name", analysis_type)
            self.assertTrue(result["success"], f"Failed for {analysis_type}")

        # 时间分析
        time_analyses = ["count", "date_range", "year_analysis", "month_analysis"]
        for analysis_type in time_analyses:
            result = self.executor.execute_analysis(
                "test_table", "timestamp", analysis_type
            )
            self.assertTrue(result["success"], f"Failed for {analysis_type}")

    def test_execute_analysis_with_limit(self):
        """测试带限制的分析执行"""
        result = self.executor.execute_analysis("test_table", "name", "top_k", limit=3)

        self.assertTrue(result["success"])
        self.assertLessEqual(len(result["result"]), 3)

    def test_execute_analysis_correlation(self):
        """测试相关性分析"""
        result = self.executor.execute_analysis(
            "test_table", "score", "correlation", second_column="rating"
        )

        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
