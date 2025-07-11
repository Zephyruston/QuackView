import json
import unittest
from datetime import datetime
from typing import Dict

import duckdb
import pandas as pd

# duckdb(v1.3.2) 数值类型
duckdb_numeric_types = {
    "bigint",
    "dec",
    "decimal",
    "double",
    "float",
    "float4",
    "float8",
    "hugeint",
    "int",
    "int1",
    "int128",
    "int16",
    "int2",
    "int32",
    "int4",
    "int64",
    "int8",
    "integer",
    "integral",
    "long",
    "numeric",
    "real",
    "short",
    "signed",
    "smallint",
    "tinyint",
    "ubigint",
    "uhugeint",
    "uint128",
    "uint16",
    "uint32",
    "uint64",
    "uint8",
    "uinteger",
    "usmallint",
    "utinyint",
    "varint",
}

# duckdb(v1.3.2) 文本类型
duckdb_text_types = {
    "bpchar",
    "char",
    "nvarchar",
    "string",
    "text",
    "varchar",
}

# duckdb(v1.3.2) 时间类型
duckdb_time_types = {
    "date",
    "datetime",
    "time",
    "timestamp",
    "timestamp_ms",
    "timestamp_ns",
    "timestamp_s",
    "timestamp_us",
    "timestamptz",
    "timetz",
    "interval",
}


def get_column_type_map(
    conn: duckdb.DuckDBPyConnection, table_name: str
) -> Dict[str, str]:
    """
    获取表的列名到类型的映射字典

    参数:
        conn: DuckDB 连接对象
        table_name: 要描述的表名

    返回:
        字典 {列名: 类型}
    """
    describe_result = conn.execute(f"DESCRIBE {table_name};").fetch_df()
    type_map = dict(
        zip(describe_result["column_name"], describe_result["column_type"].str.lower())
    )
    return type_map


def is_duckdb_numeric_type(column_type: str) -> bool:
    """
    判断 DuckDB 列类型是否为数值类型

    参数:
        column_type: DuckDB 列类型

    返回:
        bool: True 如果是数值类型, False 否则
    """
    # 处理带参数的数值类型, 如 decimal(10,2)
    base_type = column_type.split("(")[0].lower()
    return base_type in duckdb_numeric_types


def is_duckdb_text_type(column_type: str) -> bool:
    """
    判断 DuckDB 列类型是否为文本类型

    参数:
        column_type: DuckDB 列类型

    返回:
        bool: True 如果是文本类型, False 否则
    """
    return column_type in duckdb_text_types


def is_duckdb_time_type(column_type: str) -> bool:
    """
    判断 DuckDB 列类型是否为时间类型

    参数:
        column_type: DuckDB 列类型

    返回:
        bool: True 如果是时间类型, False 否则
    """
    return column_type in duckdb_time_types


class PandasJSONEncoder(json.JSONEncoder):
    """自定义JSON编码器, 处理Pandas数据类型"""

    def default(self, obj):
        if isinstance(obj, pd.Series):
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient="records")
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, pd.Timedelta):
            return str(obj)
        elif pd.isna(obj):
            return None
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif hasattr(obj, "dtype"):
            return str(obj)
        return super().default(obj)


class TestGetColumnTypeMap(unittest.TestCase):
    def setUp(self):
        """在每个测试前创建一个临时数据库和表"""
        self.conn = duckdb.connect(":memory:")
        self.conn.execute(
            """
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR,
                score FLOAT,
                timestamp TIMESTAMP,
            )
        """
        )

    def tearDown(self):
        """在每个测试后关闭连接"""
        self.conn.close()

    def test_get_column_type_map(self):
        """测试 get_column_type_map 函数"""
        expected_type_map = {
            "id": "INTEGER".lower(),
            "name": "VARCHAR".lower(),
            "score": "FLOAT".lower(),
            "timestamp": "TIMESTAMP".lower(),
        }
        result = get_column_type_map(self.conn, "test_table")
        self.assertEqual(result, expected_type_map)
        for _, type in result.items():
            if is_duckdb_numeric_type(type):
                self.assertTrue(is_duckdb_numeric_type(type))
            elif is_duckdb_text_type(type):
                self.assertTrue(is_duckdb_text_type(type))
            elif is_duckdb_time_type(type):
                self.assertTrue(is_duckdb_time_type(type))
            else:
                self.fail(f"Unknown type: {type}")


if __name__ == "__main__":
    unittest.main()
