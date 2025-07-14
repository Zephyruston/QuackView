import unittest

import duckdb
import pandas as pd

from app.generator.sql_generator import (
    AnalysisType,
    SQLGenerator,
    generate_multi_column_sql,
)


class TestSQLGenerationIntegration(unittest.TestCase):
    """测试SQL生成功能的集成测试"""

    def setUp(self):
        """在每个测试前创建测试数据库和表"""
        self.conn = duckdb.connect(":memory:")

        # 创建测试表
        self.conn.execute(
            """
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR,
                age INTEGER,
                salary DECIMAL(10,2),
                department VARCHAR,
                hire_date DATE,
                score FLOAT,
                is_active BOOLEAN
            )
        """
        )

        # 插入测试数据
        self.conn.execute(
            """
            INSERT INTO test_table VALUES 
            (1, 'Alice', 25, 50000.00, 'Engineering', '2023-01-15', 85.5, true),
            (2, 'Bob', 30, 60000.00, 'Marketing', '2023-02-20', 92.0, true),
            (3, 'Charlie', 28, 55000.00, 'Engineering', '2023-03-10', 78.5, false),
            (4, 'David', 35, 70000.00, 'Sales', '2023-04-05', 88.0, true),
            (5, 'Eve', 27, 52000.00, 'Engineering', '2023-05-12', 95.5, true),
            (6, 'Frank', 32, 65000.00, 'Marketing', '2023-06-18', 82.0, false)
        """
        )

        self.generator = SQLGenerator(self.conn, "test_table")

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_numeric_field_analysis(self):
        """测试数值字段分析"""
        # 测试基本数值分析
        analyses = [
            (AnalysisType.AVG, "SELECT AVG(age) as avg_age"),
            (AnalysisType.MAX, "SELECT MAX(age) as max_age"),
            (AnalysisType.MIN, "SELECT MIN(age) as min_age"),
            (AnalysisType.SUM, "SELECT SUM(age) as sum_age"),
            (AnalysisType.VAR_POP, "SELECT VAR_POP(age) as var_pop_age"),
            (AnalysisType.STDDEV_POP, "SELECT STDDEV_POP(age) as stddev_pop_age"),
            (AnalysisType.COUNT, "SELECT COUNT(age) as count_age"),
        ]

        for analysis_type, expected_sql in analyses:
            with self.subTest(analysis_type=analysis_type):
                sql = self.generator.generate_sql("age", analysis_type)
                self.assertIn(expected_sql.split()[1], sql)  # 检查SELECT子句

    def test_text_field_analysis(self):
        """测试文本字段分析"""
        # 测试文本分析
        analyses = [
            (AnalysisType.COUNT, "SELECT COUNT(name) as count_name"),
            (
                AnalysisType.DISTINCT_COUNT,
                "SELECT COUNT(DISTINCT name) as distinct_count_name",
            ),
            (AnalysisType.TOP_K, "SELECT name, COUNT(*) as count"),
            (AnalysisType.VALUE_DISTRIBUTION, "SELECT name, COUNT(*) as count"),
        ]

        for analysis_type, expected_sql in analyses:
            with self.subTest(analysis_type=analysis_type):
                sql = self.generator.generate_sql("name", analysis_type)
                self.assertIn(expected_sql.split()[1], sql)  # 检查SELECT子句

    def test_time_field_analysis(self):
        """测试时间字段分析"""
        # 测试时间分析
        analyses = [
            (
                AnalysisType.DATE_RANGE,
                "SELECT MIN(hire_date) as min_date, MAX(hire_date) as max_date",
            ),
            (
                AnalysisType.YEAR_ANALYSIS,
                "SELECT EXTRACT(YEAR FROM hire_date) as year, COUNT(*) as count",
            ),
            (
                AnalysisType.MONTH_ANALYSIS,
                "SELECT EXTRACT(MONTH FROM hire_date) as month, COUNT(*) as count",
            ),
        ]

        for analysis_type, expected_sql in analyses:
            with self.subTest(analysis_type=analysis_type):
                sql = self.generator.generate_sql("hire_date", analysis_type)
                self.assertIn(expected_sql.split()[1], sql)  # 检查SELECT子句

    def test_group_by_analysis(self):
        """测试分组分析"""
        sql = self.generator.generate_sql(
            "salary", AnalysisType.AVG, group_by_columns=["department"]
        )

        self.assertIn("SELECT department, AVG(salary) as avg_salary", sql)
        self.assertIn("GROUP BY department", sql)

    def test_where_conditions(self):
        """测试WHERE条件"""
        where_conditions = {"age": (">", 25), "department": ("=", "Engineering")}

        sql = self.generator.generate_sql(
            "salary", AnalysisType.AVG, where_conditions=where_conditions
        )

        self.assertIn("WHERE age > 25 AND department = 'Engineering'", sql)

    def test_between_condition(self):
        """测试BETWEEN条件"""
        where_conditions = {"age": ("BETWEEN", [25, 35])}

        sql = self.generator.generate_sql(
            "salary", AnalysisType.AVG, where_conditions=where_conditions
        )

        self.assertIn("WHERE age BETWEEN 25 AND 35", sql)

    def test_multi_column_analysis(self):
        """测试多字段分析"""
        analysis_config = {
            "age": AnalysisType.AVG,
            "salary": AnalysisType.SUM,
            "score": AnalysisType.MAX,
        }

        sql = generate_multi_column_sql(
            conn=self.conn,
            table_name="test_table",
            analysis_config=analysis_config,
            group_by_columns=["department"],
        )

        self.assertIn(
            "SELECT department, AVG(age) as avg_age, SUM(salary) as sum_salary, MAX(score) as max_score",
            sql,
        )
        self.assertIn("GROUP BY department", sql)

    def test_available_analysis_types(self):
        """测试可用分析类型获取"""
        # 数值字段
        numeric_types = self.generator.get_available_analysis_types("age")
        expected_numeric = [
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
            AnalysisType.MISSING_VALUES,
            AnalysisType.DATA_QUALITY,
        ]
        for expected_type in expected_numeric:
            self.assertIn(expected_type, numeric_types)

        # 文本字段
        text_types = self.generator.get_available_analysis_types("name")
        expected_text = [
            AnalysisType.COUNT,
            AnalysisType.DISTINCT_COUNT,
            AnalysisType.TOP_K,
            AnalysisType.VALUE_DISTRIBUTION,
            AnalysisType.LENGTH_ANALYSIS,
            AnalysisType.PATTERN_ANALYSIS,
            AnalysisType.MISSING_VALUES,
            AnalysisType.DATA_QUALITY,
        ]
        for expected_type in expected_text:
            self.assertIn(expected_type, text_types)

        # 时间字段
        time_types = self.generator.get_available_analysis_types("hire_date")
        expected_time = [
            AnalysisType.COUNT,
            AnalysisType.DATE_RANGE,
            AnalysisType.YEAR_ANALYSIS,
            AnalysisType.MONTH_ANALYSIS,
            AnalysisType.DAY_ANALYSIS,
            AnalysisType.HOUR_ANALYSIS,
            AnalysisType.WEEKDAY_ANALYSIS,
            AnalysisType.SEASONAL_ANALYSIS,
            AnalysisType.MISSING_VALUES,
            AnalysisType.DATA_QUALITY,
        ]
        for expected_type in expected_time:
            self.assertIn(expected_type, time_types)

    def test_sql_execution(self):
        """测试SQL执行"""
        # 测试基本查询执行
        sql = self.generator.generate_sql("age", AnalysisType.AVG)
        result = self.conn.execute(sql).fetch_df()

        self.assertEqual(len(result), 1)
        self.assertIn("avg_age", result.columns)
        self.assertGreater(result["avg_age"].iloc[0], 0)

        # 测试分组查询执行
        sql = self.generator.generate_sql(
            "salary", AnalysisType.AVG, group_by_columns=["department"]
        )
        result = self.conn.execute(sql).fetch_df()

        self.assertGreater(len(result), 0)
        self.assertIn("department", result.columns)
        self.assertIn("avg_salary", result.columns)

    def test_analysis_descriptions(self):
        """测试分析描述"""
        for analysis_type in AnalysisType:
            description = self.generator._get_analysis_description(analysis_type)
            self.assertIsInstance(description, str)
            self.assertGreater(len(description), 0)

    def test_sql_templates_completeness(self):
        """测试SQL模板完整性"""
        for analysis_type in AnalysisType:
            with self.subTest(analysis_type=analysis_type):
                try:
                    sql = self.generator.generate_sql("age", analysis_type)
                    self.assertIsInstance(sql, str)
                    self.assertGreater(len(sql), 0)
                    self.assertIn("SELECT", sql)
                    self.assertIn("FROM test_table", sql)
                except ValueError as e:
                    # 某些分析类型可能不支持特定字段类型
                    self.assertIn("Unsupported analysis type", str(e))


if __name__ == "__main__":
    unittest.main()
