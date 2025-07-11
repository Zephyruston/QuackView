import unittest

import duckdb

from app.executor.sql_executor import SQLExecutor
from app.generator.sql_generator import SQLGenerator


class TestEdgeCases(unittest.TestCase):
    """边界情况测试"""

    def setUp(self):
        """在每个测试前创建测试数据库和表"""
        self.conn = duckdb.connect(":memory:")

        # 创建包含边界情况的测试表
        self.conn.execute(
            """
            CREATE TABLE edge_case_table (
                id INTEGER,
                name VARCHAR,
                score FLOAT,
                price DECIMAL(10,2),
                timestamp TIMESTAMP,
                null_col VARCHAR,
                empty_string VARCHAR,
                zero_value INTEGER,
                negative_value INTEGER,
                very_large_value BIGINT,
                very_small_value DOUBLE
            )
        """
        )

        # 插入包含边界情况的测试数据
        self.conn.execute(
            """
            INSERT INTO edge_case_table VALUES 
            (1, 'Alice', 85.5, 99.99, '2023-01-01 10:00:00', NULL, '', 0, -10, 9223372036854775807, 1.0e-308),
            (2, 'Bob', 92.0, 149.99, '2023-01-02 11:00:00', NULL, '', 0, -20, 9223372036854775806, 1.0e-307),
            (3, 'Charlie', 78.5, 79.99, '2023-01-03 12:00:00', 'not_null', 'not_empty', 5, 0, 9223372036854775805, 1.0e-306),
            (4, 'David', 88.0, 129.99, '2023-01-04 13:00:00', NULL, '', 0, -5, 9223372036854775804, 1.0e-305),
            (5, 'Eve', 95.5, 199.99, '2023-01-05 14:00:00', 'not_null', 'not_empty', 10, 10, 9223372036854775803, 1.0e-304)
        """
        )

        self.generator = SQLGenerator(self.conn, "edge_case_table")
        self.executor = SQLExecutor(self.conn)

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_null_values_handling(self):
        """测试NULL值处理"""
        # 测试NULL列的分析
        result = self.executor.execute_analysis("edge_case_table", "null_col", "count")
        self.assertTrue(result["success"])

        # 测试NULL值的统计
        result = self.executor.execute_analysis(
            "edge_case_table", "null_col", "missing_values"
        )
        self.assertTrue(result["success"])
        df = result["result"]
        self.assertIn("total_count", df.columns)
        self.assertIn("null_count", df.columns)

    def test_empty_strings_handling(self):
        """测试空字符串处理"""
        # 测试空字符串列的分析
        result = self.executor.execute_analysis(
            "edge_case_table", "empty_string", "distinct_count"
        )
        self.assertTrue(result["success"])

        # 测试空字符串的值分布
        result = self.executor.execute_analysis(
            "edge_case_table", "empty_string", "value_distribution"
        )
        self.assertTrue(result["success"])

    def test_zero_values_handling(self):
        """测试零值处理"""
        # 测试零值的聚合
        result = self.executor.execute_analysis("edge_case_table", "zero_value", "avg")
        self.assertTrue(result["success"])

        # 测试零值的统计
        result = self.executor.execute_analysis("edge_case_table", "zero_value", "sum")
        self.assertTrue(result["success"])

    def test_negative_values_handling(self):
        """测试负值处理"""
        # 测试负值的聚合
        result = self.executor.execute_analysis(
            "edge_case_table", "negative_value", "avg"
        )
        self.assertTrue(result["success"])

        # 测试负值的统计
        result = self.executor.execute_analysis(
            "edge_case_table", "negative_value", "min"
        )
        self.assertTrue(result["success"])
        self.assertLess(result["result"].iloc[0, 0], 0)

    def test_very_large_values_handling(self):
        """测试极大值处理"""
        # 测试极大值的聚合
        result = self.executor.execute_analysis(
            "edge_case_table", "very_large_value", "max"
        )
        self.assertTrue(result["success"])

        # 测试极大值的统计
        result = self.executor.execute_analysis(
            "edge_case_table", "very_large_value", "avg"
        )
        self.assertTrue(result["success"])

    def test_very_small_values_handling(self):
        """测试极小值处理"""
        # 测试极小值的聚合
        result = self.executor.execute_analysis(
            "edge_case_table", "very_small_value", "min"
        )
        self.assertTrue(result["success"])

        # 测试极小值的统计
        result = self.executor.execute_analysis(
            "edge_case_table", "very_small_value", "avg"
        )
        self.assertTrue(result["success"])

    def test_empty_table_handling(self):
        """测试空表处理"""
        # 创建空表
        self.conn.execute("CREATE TABLE empty_table (id INTEGER, name VARCHAR)")

        # 测试空表的分析
        result = self.executor.execute_analysis("empty_table", "id", "count")
        self.assertTrue(result["success"])
        self.assertEqual(result["result"].iloc[0, 0], 0)

    def test_single_row_table_handling(self):
        """测试单行表处理"""
        # 创建单行表
        self.conn.execute("CREATE TABLE single_row_table (id INTEGER, name VARCHAR)")
        self.conn.execute("INSERT INTO single_row_table VALUES (1, 'test')")

        # 测试单行表的分析
        result = self.executor.execute_analysis("single_row_table", "id", "count")
        self.assertTrue(result["success"])
        self.assertEqual(result["result"].iloc[0, 0], 1)

    def test_duplicate_values_handling(self):
        """测试重复值处理"""
        # 创建包含重复值的表
        self.conn.execute(
            """
            CREATE TABLE duplicate_table (
                id INTEGER,
                name VARCHAR,
                value INTEGER
            )
        """
        )
        self.conn.execute(
            """
            INSERT INTO duplicate_table VALUES 
            (1, 'A', 10),
            (2, 'A', 10),
            (3, 'B', 20),
            (4, 'B', 20),
            (5, 'C', 30)
        """
        )

        # 测试重复值的分析
        result = self.executor.execute_analysis(
            "duplicate_table", "name", "distinct_count"
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["result"].iloc[0, 0], 3)  # 3个不同的名字

    def test_special_characters_handling(self):
        """测试特殊字符处理"""
        # 创建包含特殊字符的表
        self.conn.execute(
            """
            CREATE TABLE special_chars_table (
                id INTEGER,
                name VARCHAR,
                description VARCHAR
            )
        """
        )
        self.conn.execute(
            """
            INSERT INTO special_chars_table VALUES 
            (1, 'Test''s Name', 'Contains apostrophe'),
            (2, 'Test"s Name', 'Contains quote'),
            (3, 'Test`s Name', 'Contains backtick'),
            (4, 'Test; Name', 'Contains semicolon'),
            (5, 'Test-- Name', 'Contains comment')
        """
        )

        # 测试特殊字符的分析
        result = self.executor.execute_analysis("special_chars_table", "name", "count")
        self.assertTrue(result["success"])
        self.assertEqual(result["result"].iloc[0, 0], 5)

    def test_unicode_characters_handling(self):
        """测试Unicode字符处理"""
        # 创建包含Unicode字符的表
        self.conn.execute(
            """
            CREATE TABLE unicode_table (
                id INTEGER,
                name VARCHAR,
                description VARCHAR
            )
        """
        )
        self.conn.execute(
            """
            INSERT INTO unicode_table VALUES 
            (1, '张三', 'Chinese name'),
            (2, '李四', 'Chinese name'),
            (3, 'José', 'Spanish name'),
            (4, 'François', 'French name'),
            (5, 'Müller', 'German name')
        """
        )

        # 测试Unicode字符的分析
        result = self.executor.execute_analysis(
            "unicode_table", "name", "distinct_count"
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["result"].iloc[0, 0], 5)

    def test_extremely_long_strings_handling(self):
        """测试极长字符串处理"""
        # 创建包含极长字符串的表
        long_string = "A" * 10000  # 10KB的字符串
        self.conn.execute(
            """
            CREATE TABLE long_strings_table (
                id INTEGER,
                short_name VARCHAR,
                long_description VARCHAR
            )
        """
        )
        self.conn.execute(
            f"""
            INSERT INTO long_strings_table VALUES 
            (1, 'Short', '{long_string}'),
            (2, 'Another', '{long_string}')
        """
        )

        # 测试极长字符串的分析
        result = self.executor.execute_analysis(
            "long_strings_table", "short_name", "count"
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["result"].iloc[0, 0], 2)

    def test_numeric_precision_handling(self):
        """测试数值精度处理"""
        # 创建包含高精度数值的表
        self.conn.execute(
            """
            CREATE TABLE precision_table (
                id INTEGER,
                precise_value DECIMAL(38,10),
                float_value DOUBLE
            )
        """
        )
        self.conn.execute(
            """
            INSERT INTO precision_table VALUES 
            (1, 3.1415926535, 3.1415926535),
            (2, 2.7182818284, 2.7182818284),
            (3, 1.4142135623, 1.4142135623)
        """
        )

        # 测试高精度数值的分析
        result = self.executor.execute_analysis(
            "precision_table", "precise_value", "avg"
        )
        self.assertTrue(result["success"])

        result = self.executor.execute_analysis("precision_table", "float_value", "avg")
        self.assertTrue(result["success"])

    def test_date_edge_cases_handling(self):
        """测试日期边界情况处理"""
        # 创建包含边界日期的表
        self.conn.execute(
            """
            CREATE TABLE date_edge_table (
                id INTEGER,
                date_col DATE,
                timestamp_col TIMESTAMP
            )
        """
        )
        self.conn.execute(
            """
            INSERT INTO date_edge_table VALUES 
            (1, '1900-01-01', '1900-01-01 00:00:00'),
            (2, '2000-02-29', '2000-02-29 12:00:00'),
            (3, '2100-12-31', '2100-12-31 23:59:59'),
            (4, '2023-06-15', '2023-06-15 12:30:45')
        """
        )

        # 测试边界日期的分析
        result = self.executor.execute_analysis(
            "date_edge_table", "date_col", "date_range"
        )
        self.assertTrue(result["success"])

        result = self.executor.execute_analysis(
            "date_edge_table", "timestamp_col", "year_analysis"
        )
        self.assertTrue(result["success"])

    def test_very_large_numbers_handling(self):
        """测试极大数值处理"""
        # 创建包含极大数值的表
        self.conn.execute(
            """
            CREATE TABLE large_numbers_table (
                id INTEGER,
                huge_int BIGINT,
                huge_decimal DECIMAL(38,0)
            )
        """
        )
        self.conn.execute(
            """
            INSERT INTO large_numbers_table VALUES 
            (1, 9223372036854775807, 99999999999999999999999999999999999999),
            (2, 9223372036854775806, 99999999999999999999999999999999999998),
            (3, 9223372036854775805, 99999999999999999999999999999999999997)
        """
        )

        # 测试极大数值的分析
        result = self.executor.execute_analysis(
            "large_numbers_table", "huge_int", "max"
        )
        self.assertTrue(result["success"])

        result = self.executor.execute_analysis(
            "large_numbers_table", "huge_decimal", "max"
        )
        self.assertTrue(result["success"])

    def test_mixed_data_types_handling(self):
        """测试混合数据类型处理"""
        # 创建包含混合数据类型的表
        self.conn.execute(
            """
            CREATE TABLE mixed_types_table (
                id INTEGER,
                text_col VARCHAR,
                num_col INTEGER,
                date_col DATE,
                bool_col BOOLEAN
            )
        """
        )
        self.conn.execute(
            """
            INSERT INTO mixed_types_table VALUES 
            (1, 'Text1', 100, '2023-01-01', true),
            (2, 'Text2', 200, '2023-01-02', false),
            (3, 'Text3', 300, '2023-01-03', true)
        """
        )

        # 测试混合数据类型的分析
        result = self.executor.execute_analysis(
            "mixed_types_table", "text_col", "count"
        )
        self.assertTrue(result["success"])

        result = self.executor.execute_analysis("mixed_types_table", "num_col", "avg")
        self.assertTrue(result["success"])

        result = self.executor.execute_analysis(
            "mixed_types_table", "date_col", "date_range"
        )
        self.assertTrue(result["success"])

    def test_invalid_sql_generation(self):
        """测试无效SQL生成的处理"""
        # 测试不存在的列
        available_types = self.generator.get_available_analysis_types(
            "nonexistent_column"
        )
        self.assertEqual(available_types, [])

        # 测试无效的分析类型
        try:
            sql = self.generator.generate_sql("score", "invalid_analysis_type")
            self.fail("Should raise an exception for invalid analysis type")
        except Exception:
            pass  # 期望抛出异常

    def test_sql_execution_errors(self):
        """测试SQL执行错误处理"""
        # 测试不存在的表
        with self.assertRaises(Exception):
            self.executor.execute("SELECT * FROM nonexistent_table")

        # 测试语法错误的SQL
        with self.assertRaises(Exception):
            self.executor.execute("SELECT * FROM edge_case_table WHERE invalid_syntax")

        # 测试类型不匹配的SQL
        with self.assertRaises(Exception):
            self.executor.execute("SELECT * FROM edge_case_table WHERE name = 123")

    def test_memory_usage_with_large_data(self):
        """测试大数据量下的内存使用"""
        # 创建大数据量表
        self.conn.execute(
            """
            CREATE TABLE large_data_table (
                id INTEGER,
                name VARCHAR,
                value INTEGER
            )
        """
        )

        # 插入大量数据
        for i in range(1000):
            self.conn.execute(
                f"""
                INSERT INTO large_data_table VALUES 
                ({i}, 'Name{i}', {i * 10})
            """
            )

        # 测试大数据量的分析
        result = self.executor.execute_analysis("large_data_table", "value", "avg")
        self.assertTrue(result["success"])

        result = self.executor.execute_analysis(
            "large_data_table", "name", "distinct_count"
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["result"].iloc[0, 0], 1000)


if __name__ == "__main__":
    unittest.main()
