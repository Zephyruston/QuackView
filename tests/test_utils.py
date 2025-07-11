import unittest
from datetime import datetime

import duckdb
import pandas as pd

from app.utils.utils import (
    PandasJSONEncoder,
    get_column_type_map,
    is_duckdb_numeric_type,
    is_duckdb_text_type,
    is_duckdb_time_type,
)


class TestUtils(unittest.TestCase):
    """测试工具函数"""

    def setUp(self):
        """在每个测试前创建一个临时数据库和表"""
        self.conn = duckdb.connect(":memory:")
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
                bigint_col BIGINT,
                double_col DOUBLE
            )
        """
        )

        # 插入测试数据
        self.conn.execute(
            """
            INSERT INTO test_table VALUES 
            (1, 'Alice', 85.5, 99.99, '2023-01-01 10:00:00', '2023-01-01', 'test text', 123456789, 3.14159),
            (2, 'Bob', 92.0, 149.99, '2023-01-02 11:00:00', '2023-01-02', 'another text', 987654321, 2.71828),
            (3, 'Charlie', 78.5, 79.99, '2023-01-03 12:00:00', '2023-01-03', 'more text', 555666777, 1.41421)
        """
        )

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_get_column_type_map(self):
        """测试获取列类型映射"""
        result = get_column_type_map(self.conn, "test_table")

        # 验证基本列存在
        expected_columns = {
            "id",
            "name",
            "score",
            "price",
            "timestamp",
            "date_col",
            "text_col",
            "bigint_col",
            "double_col",
        }
        self.assertEqual(set(result.keys()), expected_columns)

        # 验证数据类型
        self.assertEqual(result["id"], "integer")
        self.assertEqual(result["name"], "varchar")
        self.assertEqual(result["score"], "float")
        self.assertEqual(result["timestamp"], "timestamp")
        self.assertEqual(result["date_col"], "date")
        # text_col可能是text或varchar，取决于DuckDB的实现
        self.assertIn(result["text_col"], ["text", "varchar"])
        self.assertEqual(result["bigint_col"], "bigint")
        self.assertEqual(result["double_col"], "double")

        # 对于decimal类型，DuckDB可能返回带精度的格式，我们只检查前缀
        self.assertTrue(result["price"].startswith("decimal"))

    def test_is_duckdb_numeric_type(self):
        """测试数值类型判断"""
        # 测试数值类型
        numeric_types = [
            "integer",
            "bigint",
            "float",
            "double",
            "decimal",
            "numeric",
            "real",
        ]
        for type_name in numeric_types:
            self.assertTrue(
                is_duckdb_numeric_type(type_name), f"{type_name} 应该是数值类型"
            )

        # 测试非数值类型
        non_numeric_types = ["varchar", "text", "timestamp", "date", "boolean"]
        for type_name in non_numeric_types:
            self.assertFalse(
                is_duckdb_numeric_type(type_name), f"{type_name} 不应该是数值类型"
            )

    def test_is_duckdb_text_type(self):
        """测试文本类型判断"""
        # 测试文本类型
        text_types = ["varchar", "text", "char", "string", "bpchar"]
        for type_name in text_types:
            self.assertTrue(
                is_duckdb_text_type(type_name), f"{type_name} 应该是文本类型"
            )

        # 测试非文本类型
        non_text_types = ["integer", "float", "timestamp", "date", "boolean"]
        for type_name in non_text_types:
            self.assertFalse(
                is_duckdb_text_type(type_name), f"{type_name} 不应该是文本类型"
            )

    def test_is_duckdb_time_type(self):
        """测试时间类型判断"""
        # 测试时间类型
        time_types = ["timestamp", "date", "time", "datetime", "timestamptz"]
        for type_name in time_types:
            self.assertTrue(
                is_duckdb_time_type(type_name), f"{type_name} 应该是时间类型"
            )

        # 测试非时间类型
        non_time_types = ["integer", "varchar", "float", "text", "boolean"]
        for type_name in non_time_types:
            self.assertFalse(
                is_duckdb_time_type(type_name), f"{type_name} 不应该是时间类型"
            )

    def test_pandas_json_encoder(self):
        """测试PandasJSONEncoder"""
        encoder = PandasJSONEncoder()

        # 测试NaN值
        self.assertIsNone(encoder.default(pd.NA))
        self.assertIsNone(encoder.default(pd.NaT))

        # 测试Timestamp
        timestamp = pd.Timestamp("2023-01-01 10:00:00")
        self.assertEqual(encoder.default(timestamp), "2023-01-01T10:00:00")

        # 测试Timedelta
        timedelta = pd.Timedelta(days=1)
        self.assertEqual(encoder.default(timedelta), "1 days 00:00:00")

        # 测试Series
        series = pd.Series([1, 2, 3])
        self.assertEqual(encoder.default(series), [1, 2, 3])

        # 测试DataFrame
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        expected = [{"a": 1, "b": 3}, {"a": 2, "b": 4}]
        self.assertEqual(encoder.default(df), expected)

        # 测试datetime对象
        dt = datetime(2023, 1, 1, 10, 0, 0)
        self.assertEqual(encoder.default(dt), "2023-01-01T10:00:00")

    def test_get_column_type_map_empty_table(self):
        """测试空表的列类型映射"""
        self.conn.execute("CREATE TABLE empty_table (id INTEGER)")
        result = get_column_type_map(self.conn, "empty_table")
        expected = {"id": "integer"}
        self.assertEqual(result, expected)

    def test_get_column_type_map_complex_types(self):
        """测试复杂类型的列类型映射"""
        self.conn.execute(
            """
            CREATE TABLE complex_table (
                id INTEGER,
                name VARCHAR(100),
                data JSON,
                array_col INTEGER[],
                struct_col STRUCT(a INTEGER, b VARCHAR)
            )
        """
        )
        result = get_column_type_map(self.conn, "complex_table")
        expected_keys = {"id", "name", "data", "array_col", "struct_col"}
        self.assertEqual(set(result.keys()), expected_keys)


if __name__ == "__main__":
    unittest.main()
