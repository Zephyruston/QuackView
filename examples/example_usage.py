"""
QuackView 使用示例
"""

import os

import pandas as pd

from app.query.engine import (
    create_memory_query_service,
    create_persistent_query_service,
)


def example_memory_mode():
    """内存模式使用示例"""
    print("=== 内存模式示例 ===")

    # 创建内存模式查询服务
    with create_memory_query_service() as service:
        # 创建示例数据
        data = {
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 28, 32],
            "score": [85, 92, 78, 88, 95],
            "department": ["IT", "HR", "IT", "Finance", "IT"],
        }
        df = pd.DataFrame(data)

        # 导入DataFrame
        result = service.import_dataframe(df, "employees")
        if result["success"]:
            print(f"✅ 成功导入数据到表: {result['table_name']}")

            # 获取表信息
            table_info = service.get_table_info()
            print(
                f"📊 表信息: {table_info['row_count']} 行, {table_info['column_count']} 列"
            )

            # 获取列类型
            column_types = service.get_column_types()
            print(f"📋 列类型: {column_types}")

            # 获取样本数据
            sample_data = service.get_sample_data(3)
            print(f"📝 样本数据:\n{sample_data}")

            # 执行分析
            analysis_result = service.execute_analysis("age", "avg")
            if analysis_result["success"]:
                print(f"📈 年龄平均值: {analysis_result['result'].iloc[0, 0]}")
                print(f"🔍 SQL: {analysis_result['sql']}")

            # 执行多字段分析
            multi_result = service.execute_multi_column_analysis(
                {"age": "avg", "score": "max"}
            )
            if multi_result["success"]:
                print(f"📊 多字段分析结果:\n{multi_result['result']}")
                print(f"🔍 SQL: {multi_result['sql']}")

            # 获取字段可用的分析方法
            available_analyses = service.get_available_analyses("score")
            print(f"🎯 分数字段可用分析: {available_analyses}")

            # 执行自定义SQL
            custom_result = service.execute_custom_sql(
                "SELECT department, AVG(score) as avg_score FROM employees GROUP BY department"
            )
            if custom_result["success"]:
                print(f"🔧 自定义SQL结果:\n{custom_result['result']}")
        else:
            print(f"❌ 导入失败: {result['error']}")


def example_persistent_mode():
    """持久化模式使用示例"""
    print("\n=== 持久化模式示例 ===")

    # 创建持久化模式查询服务
    with create_persistent_query_service("test_db.duckdb") as service:
        # 创建示例数据
        data = {
            "product": ["A", "B", "C", "A", "B", "C"],
            "sales": [100, 150, 200, 120, 180, 220],
            "region": ["North", "South", "North", "South", "North", "South"],
        }
        df = pd.DataFrame(data)

        # 导入DataFrame
        result = service.import_dataframe(df, "sales_data")
        if result["success"]:
            print(f"✅ 成功导入数据到表: {result['table_name']}")

            # 执行分组分析
            group_result = service.execute_analysis(
                "sales", "sum", group_by_columns=["region"]
            )
            if group_result["success"]:
                print(f"📊 按地区销售总额:\n{group_result['result']}")
                print(f"🔍 SQL: {group_result['sql']}")

            # 执行条件分析
            condition_result = service.execute_analysis(
                "sales", "avg", where_conditions={"region": ("=", "North")}
            )
            if condition_result["success"]:
                print(f"📈 北方地区平均销售额: {condition_result['result'].iloc[0, 0]}")
                print(f"🔍 SQL: {condition_result['sql']}")

            # 获取快速分析
            quick_analysis = service.get_quick_analysis()
            print(f"⚡ 快速分析结果: {quick_analysis}")
        else:
            print(f"❌ 导入失败: {result['error']}")


def example_excel_import():
    """Excel文件导入示例"""
    print("\n=== Excel文件导入示例 ===")

    # 检查是否有Excel文件
    excel_file = "data.xlsx"
    if not os.path.exists(excel_file):
        print(f"⚠️  Excel文件 {excel_file} 不存在，跳过此示例")
        return

    with create_memory_query_service() as service:
        # 导入Excel文件
        result = service.import_excel(excel_file)
        if result["success"]:
            print(f"✅ 成功导入Excel文件: {result['table_name']}")

            # 获取分析信息
            analysis_info = result["analysis_info"]
            print(f"📊 表信息: {analysis_info['table_info']['row_count']} 行")
            print(f"📋 列类型: {analysis_info['column_types']}")

            # 显示每个字段的分析选项
            for column, options in analysis_info["analysis_options"].items():
                print(f"🎯 {column} 字段可用分析: {[opt['type'] for opt in options]}")

            # 执行一些示例分析
            for column in analysis_info["column_types"].keys():
                default_analysis = service.get_default_analysis(column)
                print(f"📈 {column} 默认分析: {default_analysis}")

                # 执行默认分析
                analysis_result = service.execute_analysis(column, default_analysis)
                if analysis_result["success"]:
                    print(f"✅ {column} 分析成功")
                else:
                    print(f"❌ {column} 分析失败: {analysis_result['error']}")
        else:
            print(f"❌ Excel导入失败: {result['error']}")


if __name__ == "__main__":
    print("🚀 QuackView 使用示例")
    print("=" * 50)

    example_memory_mode()
    example_persistent_mode()
    example_excel_import()

    print("\n✅ 所有示例运行完成！")
