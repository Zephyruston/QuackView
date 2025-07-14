import unittest

import duckdb

from app.generator.sql_generator import (
    AnalysisType,
    SQLGenerator,
    generate_multi_column_sql,
)


class TestSQLGenerator(unittest.TestCase):
    """测试SQL生成器"""

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
                price DECIMAL(10,2),
                timestamp TIMESTAMP,
                date_col DATE,
                text_col TEXT,
                category VARCHAR,
                amount INTEGER,
                rating DOUBLE
            )
        """
        )

        # 插入测试数据
        self.conn.execute(
            """
            INSERT INTO test_table VALUES 
            (1, 'Alice', 85.5, 99.99, '2023-01-01 10:00:00', '2023-01-01', 'test text', 'A', 100, 4.5),
            (2, 'Bob', 92.0, 149.99, '2023-01-02 11:00:00', '2023-01-02', 'another text', 'B', 200, 4.8),
            (3, 'Charlie', 78.5, 79.99, '2023-01-03 12:00:00', '2023-01-03', 'more text', 'A', 150, 4.2),
            (4, 'David', 88.0, 129.99, '2023-02-01 13:00:00', '2023-02-01', 'sample text', 'C', 300, 4.6),
            (5, 'Eve', 95.5, 199.99, '2023-02-02 14:00:00', '2023-02-02', 'example text', 'B', 250, 4.9),
            (6, 'Frank', 82.0, 89.99, '2023-03-01 15:00:00', '2023-03-01', 'final text', 'A', 180, 4.3)
        """
        )

        self.generator = SQLGenerator(self.conn, "test_table")

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_get_available_analysis_types_numeric(self):
        """测试数值字段的可用分析类型"""
        available_types = self.generator.get_available_analysis_types("score")
        expected_types = [
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
        self.assertEqual(set(available_types), set(expected_types))

    def test_get_available_analysis_types_text(self):
        """测试文本字段的可用分析类型"""
        available_types = self.generator.get_available_analysis_types("name")
        expected_types = [
            AnalysisType.COUNT,
            AnalysisType.DISTINCT_COUNT,
            AnalysisType.TOP_K,
            AnalysisType.VALUE_DISTRIBUTION,
            AnalysisType.LENGTH_ANALYSIS,
            AnalysisType.PATTERN_ANALYSIS,
            AnalysisType.MISSING_VALUES,
            AnalysisType.DATA_QUALITY,
        ]
        self.assertEqual(set(available_types), set(expected_types))

    def test_get_available_analysis_types_time(self):
        """测试时间字段的可用分析类型"""
        available_types = self.generator.get_available_analysis_types("timestamp")
        expected_types = [
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
        self.assertEqual(set(available_types), set(expected_types))

    def test_get_available_analysis_types_nonexistent_column(self):
        """测试不存在的列"""
        available_types = self.generator.get_available_analysis_types("nonexistent")
        self.assertEqual(available_types, [])

    def test_generate_sql_basic_analysis(self):
        """测试基本分析SQL生成"""
        # 测试平均值
        sql = self.generator.generate_sql("score", AnalysisType.AVG)
        self.assertIn("SELECT AVG(score) as avg_score", sql)
        self.assertIn("FROM test_table", sql)

        # 测试最大值
        sql = self.generator.generate_sql("score", AnalysisType.MAX)
        self.assertIn("SELECT MAX(score) as max_score", sql)

        # 测试最小值
        sql = self.generator.generate_sql("score", AnalysisType.MIN)
        self.assertIn("SELECT MIN(score) as min_score", sql)

        # 测试总和
        sql = self.generator.generate_sql("score", AnalysisType.SUM)
        self.assertIn("SELECT SUM(score) as sum_score", sql)

    def test_generate_sql_statistical_analysis(self):
        """测试统计分析SQL生成"""
        # 测试方差
        sql = self.generator.generate_sql("score", AnalysisType.VAR_POP)
        self.assertIn("SELECT VAR_POP(score) as var_pop_score", sql)

        # 测试标准差
        sql = self.generator.generate_sql("score", AnalysisType.STDDEV_POP)
        self.assertIn("SELECT STDDEV_POP(score) as stddev_pop_score", sql)

        # 测试中位数
        sql = self.generator.generate_sql("score", AnalysisType.MEDIAN)
        self.assertIn("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score)", sql)

    def test_generate_sql_text_analysis(self):
        """测试文本分析SQL生成"""
        # 测试唯一值计数
        sql = self.generator.generate_sql("name", AnalysisType.DISTINCT_COUNT)
        self.assertIn("SELECT COUNT(DISTINCT name) as distinct_count_name", sql)

        # 测试TOP-K
        sql = self.generator.generate_sql("name", AnalysisType.TOP_K)
        self.assertIn("SELECT name, COUNT(*) as count", sql)
        self.assertIn("GROUP BY name", sql)
        self.assertIn("ORDER BY count DESC", sql)

        # 测试值分布
        sql = self.generator.generate_sql("name", AnalysisType.VALUE_DISTRIBUTION)
        self.assertIn("SELECT name, COUNT(*) as count", sql)
        self.assertIn("GROUP BY name", sql)

    def test_generate_sql_time_analysis(self):
        """测试时间分析SQL生成"""
        # 测试日期范围
        sql = self.generator.generate_sql("timestamp", AnalysisType.DATE_RANGE)
        self.assertIn(
            "SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date", sql
        )

        # 测试年度分析
        sql = self.generator.generate_sql("timestamp", AnalysisType.YEAR_ANALYSIS)
        self.assertIn(
            "SELECT EXTRACT(YEAR FROM timestamp) as year, COUNT(*) as count", sql
        )

        # 测试月度分析
        sql = self.generator.generate_sql("timestamp", AnalysisType.MONTH_ANALYSIS)
        self.assertIn(
            "SELECT EXTRACT(MONTH FROM timestamp) as month, COUNT(*) as count", sql
        )

    def test_generate_sql_with_where_conditions(self):
        """测试带WHERE条件的SQL生成"""
        where_conditions = {"score": (">", 80), "category": ("=", "A")}
        sql = self.generator.generate_sql(
            "score", AnalysisType.AVG, where_conditions=where_conditions
        )
        self.assertIn("WHERE score > 80 AND category = 'A'", sql)

    def test_generate_sql_with_group_by(self):
        """测试带GROUP BY的SQL生成"""
        group_by_columns = ["category"]
        sql = self.generator.generate_sql(
            "score", AnalysisType.AVG, group_by_columns=group_by_columns
        )
        self.assertIn("GROUP BY category", sql)

    def test_generate_sql_with_limit(self):
        """测试带LIMIT的SQL生成"""
        sql = self.generator.generate_sql("name", AnalysisType.TOP_K, limit=5)
        self.assertIn("LIMIT 5", sql)

    def test_generate_sql_correlation(self):
        """测试相关性分析SQL生成"""
        sql = self.generator.generate_sql(
            "score", AnalysisType.CORRELATION, second_column="rating"
        )
        self.assertIn("SELECT CORR(score, rating) as correlation", sql)

    def test_generate_sql_missing_values(self):
        """测试缺失值分析SQL生成"""
        sql = self.generator.generate_sql("score", AnalysisType.MISSING_VALUES)
        self.assertIn(
            "SELECT COUNT(*) as total_count, COUNT(score) as non_null_count", sql
        )

    def test_generate_sql_data_quality(self):
        """测试数据质量分析SQL生成"""
        sql = self.generator.generate_sql("score", AnalysisType.DATA_QUALITY)
        self.assertIn(
            "SELECT COUNT(*) as total_count, COUNT(score) as non_null_count", sql
        )

    def test_get_analysis_examples(self):
        """测试获取分析示例"""
        examples = self.generator.get_analysis_examples("score")
        self.assertIsInstance(examples, list)
        self.assertGreater(len(examples), 0)

        for example in examples:
            self.assertIn("type", example)
            self.assertIn("description", example)
            self.assertIn("sql", example)

    def test_sql_templates_completeness(self):
        """测试SQL模板的完整性"""
        # 检查所有分析类型都有对应的模板
        for analysis_type in AnalysisType:
            sql = self.generator.generate_sql("score", analysis_type)
            self.assertIsInstance(sql, str)
            self.assertGreater(len(sql), 0)

    def test_analysis_descriptions_completeness(self):
        """测试分析描述的完整性"""
        for analysis_type in AnalysisType:
            description = self.generator._get_analysis_description(analysis_type)
            self.assertIsInstance(description, str)
            self.assertGreater(len(description), 0)


class TestMultiColumnSQLGenerator(unittest.TestCase):
    """测试多列SQL生成器"""

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
                amount INTEGER
            )
        """
        )

        # 插入测试数据
        self.conn.execute(
            """
            INSERT INTO test_table VALUES 
            (1, 'Alice', 85.5, 'A', 100),
            (2, 'Bob', 92.0, 'B', 200),
            (3, 'Charlie', 78.5, 'A', 150)
        """
        )

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_generate_multi_column_sql(self):
        """测试多列SQL生成"""
        from app.generator.sql_generator import AnalysisType

        analysis_config = {
            "score": AnalysisType.AVG,
            "amount": AnalysisType.SUM,
            "name": AnalysisType.COUNT,
        }

        sql = generate_multi_column_sql(self.conn, "test_table", analysis_config)

        self.assertIn("SELECT", sql)
        self.assertIn("AVG(score)", sql)
        self.assertIn("SUM(amount)", sql)
        self.assertIn("COUNT(name)", sql)
        self.assertIn("FROM test_table", sql)

    def test_generate_multi_column_sql_with_group_by(self):
        """测试带GROUP BY的多列SQL生成"""
        from app.generator.sql_generator import AnalysisType

        analysis_config = {"score": AnalysisType.AVG, "amount": AnalysisType.SUM}

        group_by_columns = ["category"]

        sql = generate_multi_column_sql(
            self.conn, "test_table", analysis_config, group_by_columns=group_by_columns
        )

        self.assertIn("GROUP BY category", sql)

    def test_generate_multi_column_sql_with_where(self):
        """测试带WHERE条件的多列SQL生成"""
        from app.generator.sql_generator import AnalysisType

        analysis_config = {"score": AnalysisType.AVG, "amount": AnalysisType.SUM}

        where_conditions = {"score": (">", 80)}

        sql = generate_multi_column_sql(
            self.conn, "test_table", analysis_config, where_conditions=where_conditions
        )

        self.assertIn("WHERE score > 80", sql)


if __name__ == "__main__":
    unittest.main()
