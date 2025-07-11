import unittest

import duckdb
import pandas as pd

from app.executor.sql_executor import SQLExecutor
from app.generator.sql_generator import AnalysisType, SQLGenerator
from app.utils.utils import get_column_type_map


class TestIntegration(unittest.TestCase):
    """集成测试：测试整个工作流程"""

    def setUp(self):
        """在每个测试前创建测试数据库和表"""
        self.conn = duckdb.connect(":memory:")

        # 创建测试表
        self.conn.execute(
            """
            CREATE TABLE sales_data (
                id INTEGER,
                product_name VARCHAR,
                category VARCHAR,
                price DECIMAL(10,2),
                quantity INTEGER,
                sale_date DATE,
                customer_id INTEGER,
                region VARCHAR,
                rating DOUBLE
            )
        """
        )

        # 插入测试数据
        self.conn.execute(
            """
            INSERT INTO sales_data VALUES 
            (1, 'Laptop', 'Electronics', 999.99, 2, '2023-01-15', 101, 'North', 4.5),
            (2, 'Phone', 'Electronics', 599.99, 1, '2023-01-20', 102, 'South', 4.8),
            (3, 'Book', 'Books', 29.99, 3, '2023-02-01', 103, 'East', 4.2),
            (4, 'Tablet', 'Electronics', 399.99, 1, '2023-02-10', 104, 'West', 4.6),
            (5, 'Chair', 'Furniture', 199.99, 2, '2023-02-15', 105, 'North', 4.3),
            (6, 'Desk', 'Furniture', 299.99, 1, '2023-03-01', 106, 'South', 4.7),
            (7, 'Headphones', 'Electronics', 89.99, 2, '2023-03-10', 107, 'East', 4.4),
            (8, 'Pen', 'Office', 9.99, 10, '2023-03-15', 108, 'West', 4.1),
            (9, 'Monitor', 'Electronics', 299.99, 1, '2023-04-01', 109, 'North', 4.9),
            (10, 'Keyboard', 'Electronics', 79.99, 2, '2023-04-10', 110, 'South', 4.0)
        """
        )

        self.generator = SQLGenerator(self.conn, "sales_data")
        self.executor = SQLExecutor(self.conn)

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_complete_workflow_numeric_analysis(self):
        """测试完整的数值分析工作流程"""
        # 1. 获取列类型映射
        column_types = get_column_type_map(self.conn, "sales_data")
        self.assertIn("price", column_types)
        self.assertTrue(column_types["price"].startswith("decimal"))

        # 2. 获取可用分析类型
        available_types = self.generator.get_available_analysis_types("price")
        self.assertIn(AnalysisType.AVG, available_types)
        self.assertIn(AnalysisType.MAX, available_types)

        # 3. 生成SQL
        sql = self.generator.generate_sql("price", AnalysisType.AVG)
        self.assertIn("SELECT AVG(price) as avg_price", sql)
        self.assertIn("FROM sales_data", sql)

        # 4. 执行SQL
        result = self.executor.execute(sql)
        self.assertEqual(len(result), 1)
        self.assertIn("avg_price", result.columns)
        self.assertGreater(result.iloc[0, 0], 0)

    def test_complete_workflow_text_analysis(self):
        """测试完整的文本分析工作流程"""
        # 1. 获取可用分析类型
        available_types = self.generator.get_available_analysis_types("product_name")
        self.assertIn(AnalysisType.DISTINCT_COUNT, available_types)
        self.assertIn(AnalysisType.TOP_K, available_types)

        # 2. 生成SQL
        sql = self.generator.generate_sql("product_name", AnalysisType.DISTINCT_COUNT)
        self.assertIn(
            "SELECT COUNT(DISTINCT product_name) as distinct_count_product_name", sql
        )

        # 3. 执行SQL
        result = self.executor.execute(sql)
        self.assertEqual(len(result), 1)
        self.assertIn("distinct_count_product_name", result.columns)

    def test_complete_workflow_time_analysis(self):
        """测试完整的时间分析工作流程"""
        # 1. 获取可用分析类型
        available_types = self.generator.get_available_analysis_types("sale_date")
        self.assertIn(AnalysisType.DATE_RANGE, available_types)
        self.assertIn(AnalysisType.MONTH_ANALYSIS, available_types)

        # 2. 生成SQL
        sql = self.generator.generate_sql("sale_date", AnalysisType.DATE_RANGE)
        self.assertIn(
            "SELECT MIN(sale_date) as min_date, MAX(sale_date) as max_date", sql
        )

        # 3. 执行SQL
        result = self.executor.execute(sql)
        self.assertEqual(len(result), 1)
        self.assertIn("min_date", result.columns)
        self.assertIn("max_date", result.columns)

    def test_complete_workflow_with_conditions(self):
        """测试带条件的完整工作流程"""
        # 1. 设置条件
        where_conditions = {"category": ("=", "Electronics"), "price": (">", 100)}

        # 2. 生成SQL
        sql = self.generator.generate_sql(
            "price", AnalysisType.AVG, where_conditions=where_conditions
        )
        self.assertIn("WHERE category = 'Electronics' AND price > 100", sql)

        # 3. 执行SQL
        result = self.executor.execute(sql)
        self.assertEqual(len(result), 1)
        self.assertIn("avg_price", result.columns)

    def test_complete_workflow_with_group_by(self):
        """测试带分组的完整工作流程"""
        # 1. 设置分组
        group_by_columns = ["category"]

        # 2. 生成SQL
        sql = self.generator.generate_sql(
            "price", AnalysisType.AVG, group_by_columns=group_by_columns
        )
        self.assertIn("GROUP BY category", sql)

        # 3. 执行SQL
        result = self.executor.execute(sql)
        self.assertGreater(len(result), 1)  # 应该有多个类别
        self.assertIn("category", result.columns)

    def test_complete_workflow_multi_column_analysis(self):
        """测试多列分析的完整工作流程"""
        from app.generator.sql_generator import generate_multi_column_sql

        # 1. 设置分析配置
        analysis_config = {
            "price": AnalysisType.AVG,
            "quantity": AnalysisType.SUM,
            "product_name": AnalysisType.COUNT,
        }

        # 2. 生成SQL
        sql = generate_multi_column_sql(self.conn, "sales_data", analysis_config)
        self.assertIn("AVG(price)", sql)
        self.assertIn("SUM(quantity)", sql)
        self.assertIn("COUNT(product_name)", sql)

        # 3. 执行SQL
        result = self.executor.execute(sql)
        self.assertEqual(len(result), 1)
        self.assertIn("avg_price", result.columns)
        self.assertIn("sum_quantity", result.columns)
        self.assertIn("count_product_name", result.columns)

    def test_complete_workflow_executor_analysis(self):
        """测试执行器分析方法的完整工作流程"""
        # 1. 执行分析
        result = self.executor.execute_analysis(
            "sales_data",
            "price",
            "avg",
            where_conditions={"category": ("=", "Electronics")},
        )

        # 2. 验证结果
        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)
        self.assertIsInstance(result["sql"], str)
        self.assertIsNone(result["error"])

        # 3. 验证SQL内容
        sql = result["sql"]
        self.assertIn("AVG(price) as avg_price", sql)
        self.assertIn("WHERE category = 'Electronics'", sql)

    def test_complete_workflow_multi_column_executor(self):
        """测试多列执行器分析的完整工作流程"""
        # 1. 设置分析配置
        analysis_config = {"price": "avg", "quantity": "sum", "product_name": "count"}

        # 2. 执行分析
        result = self.executor.execute_multi_column_analysis(
            "sales_data", analysis_config, group_by_columns=["category"]
        )

        # 3. 验证结果
        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)
        self.assertIn("category", result["result"].columns)
        self.assertGreater(len(result["result"]), 1)

    def test_complete_workflow_error_handling(self):
        """测试错误处理的完整工作流程"""
        # 1. 测试不存在的列
        available_types = self.generator.get_available_analysis_types(
            "nonexistent_column"
        )
        self.assertEqual(available_types, [])

        # 2. 测试无效的分析类型
        result = self.executor.execute_analysis(
            "sales_data", "price", "invalid_analysis_type"
        )
        self.assertFalse(result["success"])
        self.assertIn("不支持的分析类型", result["error"])

        # 3. 测试不存在的表
        schema = self.executor.get_table_schema("nonexistent_table")
        self.assertEqual(schema, {})

    def test_complete_workflow_complex_analysis(self):
        """测试复杂分析的完整工作流程"""
        # 1. 设置复杂条件
        where_conditions = {"price": (">", 100), "category": ("=", "Electronics")}
        group_by_columns = ["region"]

        # 2. 生成SQL
        sql = self.generator.generate_sql(
            "price",
            AnalysisType.AVG,
            where_conditions=where_conditions,
            group_by_columns=group_by_columns,
        )

        # 3. 验证SQL结构
        self.assertIn("AVG(price) as avg_price", sql)
        self.assertIn("WHERE price > 100 AND category = 'Electronics'", sql)
        self.assertIn("GROUP BY region", sql)

        # 4. 执行SQL
        result = self.executor.execute(sql)
        self.assertGreater(len(result), 0)
        self.assertIn("avg_price", result.columns)
        self.assertIn("region", result.columns)

    def test_complete_workflow_all_analysis_types(self):
        """测试所有分析类型的完整工作流程"""
        # 数值分析
        numeric_columns = ["price", "quantity", "rating"]
        numeric_analyses = ["avg", "max", "min", "sum", "count", "median"]

        for column in numeric_columns:
            for analysis in numeric_analyses:
                result = self.executor.execute_analysis("sales_data", column, analysis)
                self.assertTrue(result["success"], f"Failed for {column} - {analysis}")

        # 文本分析
        text_columns = ["product_name", "category", "region"]
        text_analyses = ["count", "distinct_count", "top_k"]

        for column in text_columns:
            for analysis in text_analyses:
                result = self.executor.execute_analysis("sales_data", column, analysis)
                self.assertTrue(result["success"], f"Failed for {column} - {analysis}")

        # 时间分析
        time_columns = ["sale_date"]
        time_analyses = ["count", "date_range", "month_analysis"]

        for column in time_columns:
            for analysis in time_analyses:
                result = self.executor.execute_analysis("sales_data", column, analysis)
                self.assertTrue(result["success"], f"Failed for {column} - {analysis}")

    def test_complete_workflow_data_quality(self):
        """测试数据质量分析的完整工作流程"""
        # 1. 执行数据质量分析
        result = self.executor.execute_analysis("sales_data", "price", "data_quality")

        # 2. 验证结果
        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)

        # 3. 验证数据质量指标
        df = result["result"]
        self.assertIn("total_count", df.columns)
        self.assertIn("non_null_count", df.columns)
        self.assertIn("distinct_count", df.columns)

        # 4. 验证数据合理性
        total_count = df.iloc[0]["total_count"]
        non_null_count = df.iloc[0]["non_null_count"]
        distinct_count = df.iloc[0]["distinct_count"]

        self.assertEqual(total_count, 10)  # 总记录数
        self.assertEqual(non_null_count, 10)  # 非空记录数
        self.assertGreater(distinct_count, 0)  # 唯一值数

    def test_complete_workflow_correlation_analysis(self):
        """测试相关性分析的完整工作流程"""
        # 1. 执行相关性分析
        result = self.executor.execute_analysis(
            "sales_data", "price", "correlation", second_column="rating"
        )

        # 2. 验证结果
        self.assertTrue(result["success"])
        self.assertIsInstance(result["result"], pd.DataFrame)

        # 3. 验证相关性列
        df = result["result"]
        self.assertIn("correlation", df.columns)

        # 4. 验证相关性值在合理范围内
        correlation_value = df.iloc[0]["correlation"]
        self.assertIsNotNone(correlation_value)
        self.assertGreaterEqual(correlation_value, -1)
        self.assertLessEqual(correlation_value, 1)


if __name__ == "__main__":
    unittest.main()
