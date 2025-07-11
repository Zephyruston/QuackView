"""
QuackView CLI - Excel数据分析工具
"""

import json
import sys
from typing import Dict, Optional

import click
import pandas as pd
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import IntPrompt, Prompt
from rich.table import Table

from ..query import create_memory_query_service
from ..utils.utils import (
    PandasJSONEncoder,
    is_duckdb_numeric_type,
    is_duckdb_text_type,
    is_duckdb_time_type,
)

console = Console()


def print_banner():
    """打印欢迎横幅"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                    🦆 QuackView CLI                          ║
    ║                 Excel数据分析工具 v1.0                       ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    console.print(Panel(banner, style="bold blue"))


def print_usage_examples():
    """打印使用示例"""
    examples = """
    📖 使用示例:
    
    🔍 交互式分析Excel文件:
       qv analyze data.xlsx
       qv analyze data.xlsx --sheet-name "Sheet2"
       qv analyze data.xlsx --head 10 --json
    
    📊 查看表结构:
       qv schema data.xlsx
       qv schema data.xlsx --sheet-name "Sheet2" --json
    
    🔍 执行SQL查询:
       qv query data.xlsx "SELECT * FROM data LIMIT 10"
       qv query data.xlsx "SELECT COUNT(*) FROM data" --json
    
    📋 查看版本信息:
       qv version
    
    💡 提示:
    - 支持 .xlsx, .xls 格式的Excel文件
    - 使用 --json 参数可获取机器可读的输出
    - 使用 --sheet-name 指定工作表(默认第一个)
    - 使用 --head 参数控制显示的行数
    """
    console.print(Panel(examples, title="📚 使用指南", style="cyan"))


def display_table_info(table_info: Dict) -> None:
    """显示表结构信息"""
    if not table_info:
        console.print("❌ 无法获取表信息", style="red")
        return

    table = Table(title="📊 数据表结构", show_header=True, header_style="bold magenta")
    table.add_column("列名", style="cyan", no_wrap=True)
    table.add_column("类型", style="green")
    table.add_column("可空", style="yellow")
    table.add_column("默认值", style="blue")

    for column in table_info.get("columns", []):
        table.add_row(
            column["name"],
            column["type"],
            "是" if column["null"] else "否",
            str(column["default"]) if column["default"] else "-",
        )

    console.print(table)
    console.print(f"📈 总行数: {table_info.get('row_count', 0)}", style="bold green")


def display_sample_data(df: pd.DataFrame, limit: int = 5) -> None:
    """显示样本数据"""
    if df.empty:
        console.print("❌ 没有可显示的样本数据", style="red")
        return

    display_df = df.head(limit)

    column_widths = {}
    for col in display_df.columns:
        col_name_width = len(str(col))
        data_widths = []
        for val in display_df[col].head(limit):
            str_val = str(val)
            if len(str_val) > 30:
                data_widths.append(30)
            else:
                data_widths.append(len(str_val))

        max_data_width = max(data_widths) if data_widths else 10
        column_widths[col] = max(col_name_width, max_data_width, 12)

    total_width = sum(column_widths.values()) + len(column_widths) * 2
    console_width = 150

    if total_width > console_width:
        avg_col_width = sum(column_widths.values()) / len(column_widths)
        cols_per_row = max(1, int((console_width - 20) / (avg_col_width + 2)))

        columns_list = list(display_df.columns)
        column_groups = []
        for i in range(0, len(columns_list), cols_per_row):
            column_groups.append(columns_list[i : i + cols_per_row])

        for group_idx, column_group in enumerate(column_groups):
            group_df = display_df[column_group]

            group_widths = {col: column_widths[col] for col in column_group}
            group_total_width = sum(group_widths.values()) + len(group_widths) * 2

            table = Table(
                title=f"📋 样本数据 (前{len(display_df)}行) - 第{group_idx + 1}组",
                show_header=True,
                header_style="bold magenta",
                width=min(group_total_width, console_width),
                show_edge=True,
                box=box.ROUNDED,
            )

            for col in column_group:
                width = group_widths[col]
                col_name = str(col)
                if len(col_name) > width:
                    col_name = col_name[: width - 3] + "..."

                table.add_column(col_name, style="cyan", width=width, no_wrap=False)

            for _, row in group_df.iterrows():
                formatted_row = []
                for col in column_group:
                    val = row[col]
                    str_val = str(val)

                    max_width = group_widths[col]
                    if len(str_val) > max_width:
                        formatted_val = str_val[: max_width - 3] + "..."
                    else:
                        formatted_val = str_val
                    formatted_row.append(formatted_val)

                table.add_row(*formatted_row)

            console.print(table)
            if group_idx < len(column_groups) - 1:
                console.print()
    else:
        table = Table(
            title=f"📋 样本数据 (前{len(display_df)}行)",
            show_header=True,
            header_style="bold magenta",
            width=min(total_width, console_width),
            show_edge=True,
            box=box.ROUNDED,
        )

        for col in display_df.columns:
            width = column_widths.get(col, 15)
            col_name = str(col)
            if len(col_name) > width:
                col_name = col_name[: width - 3] + "..."

            table.add_column(col_name, style="cyan", width=width, no_wrap=False)

        for _, row in display_df.iterrows():
            formatted_row = []
            for col in display_df.columns:
                val = row[col]
                str_val = str(val)

                max_width = column_widths.get(col, 15)
                if len(str_val) > max_width:
                    formatted_val = str_val[: max_width - 3] + "..."
                else:
                    formatted_val = str_val
                formatted_row.append(formatted_val)

            table.add_row(*formatted_row)

        console.print(table)


def get_analysis_options(service) -> Dict:
    """获取分析选项"""
    column_types = service.get_column_types()
    if not column_types:
        console.print("❌ 无法获取列信息", style="red")
        return {}

    console.print("\n📋 可用列:", style="bold")
    for i, (col_name, col_type) in enumerate(column_types.items(), 1):
        console.print(f"  {i}. {col_name} ({col_type})", style="cyan")

    while True:
        try:
            choice = IntPrompt.ask("请选择要分析的列 (输入序号)", default=1)
            if 1 <= choice <= len(column_types):
                selected_col = list(column_types.keys())[choice - 1]
                break
            else:
                console.print("❌ 无效选择, 请重试", style="red")
        except ValueError:
            console.print("❌ 请输入有效数字", style="red")

    available_analyses = service.get_available_analyses(selected_col)

    console.print(f"\n🔍 列 '{selected_col}' 可用的分析方法:", style="bold")
    for i, analysis in enumerate(available_analyses, 1):
        console.print(f"  {i}. {analysis}", style="cyan")

    while True:
        try:
            analysis_choice = IntPrompt.ask("请选择分析方法 (输入序号)", default=1)
            if 1 <= analysis_choice <= len(available_analyses):
                selected_analysis = available_analyses[analysis_choice - 1]
                break
            else:
                console.print("❌ 无效选择, 请重试", style="red")
        except ValueError:
            console.print("❌ 请输入有效数字", style="red")

    return {"column_name": selected_col, "analysis_type": selected_analysis}


def display_analysis_result(result: Dict) -> None:
    """显示分析结果"""
    if not result.get("success"):
        console.print(f"❌ 分析失败: {result.get('error', '未知错误')}", style="red")
        return

    console.print("\n✅ 分析结果:", style="bold green")

    if result.get("sql"):
        console.print(f"🔍 执行的SQL: {result['sql']}", style="yellow")

    result_data = result.get("result")
    if result_data is not None:
        df = result_data
        if isinstance(df, pd.DataFrame) and not df.empty:
            column_widths = {}
            for col in df.columns:
                col_name_width = len(str(col))

                data_widths = []
                for val in df[col].head(10):
                    str_val = str(val)
                    if len(str_val) > 30:
                        data_widths.append(30)
                    else:
                        data_widths.append(len(str_val))

                max_data_width = max(data_widths) if data_widths else 10
                column_widths[col] = max(col_name_width, max_data_width, 12)

            total_width = sum(column_widths.values()) + len(column_widths) * 2
            console_width = 150

            if total_width > console_width:
                avg_col_width = sum(column_widths.values()) / len(column_widths)
                cols_per_row = max(1, int((console_width - 20) / (avg_col_width + 2)))

                columns_list = list(df.columns)
                column_groups = []
                for i in range(0, len(columns_list), cols_per_row):
                    column_groups.append(columns_list[i : i + cols_per_row])

                for group_idx, column_group in enumerate(column_groups):
                    group_df = df[column_group]

                    group_widths = {col: column_widths[col] for col in column_group}
                    group_total_width = (
                        sum(group_widths.values()) + len(group_widths) * 2
                    )

                    table = Table(
                        title=f"📊 分析结果 - 第{group_idx + 1}组",
                        show_header=True,
                        header_style="bold magenta",
                        width=min(group_total_width, console_width),
                        show_edge=True,
                        box=box.ROUNDED,
                    )

                    for col in column_group:
                        width = group_widths[col]
                        col_name = str(col)
                        if len(col_name) > width:
                            col_name = col_name[: width - 3] + "..."

                        table.add_column(
                            col_name, style="cyan", width=width, no_wrap=False
                        )

                    for _, row in group_df.iterrows():
                        formatted_row = []
                        for col in column_group:
                            val = row[col]
                            str_val = str(val)

                            max_width = group_widths[col]
                            if len(str_val) > max_width:
                                formatted_val = str_val[: max_width - 3] + "..."
                            else:
                                formatted_val = str_val
                            formatted_row.append(formatted_val)

                        table.add_row(*formatted_row)

                    console.print(table)
                    if group_idx < len(column_groups) - 1:
                        console.print()
            else:
                table = Table(
                    title="📊 分析结果",
                    show_header=True,
                    header_style="bold magenta",
                    width=min(total_width, console_width),
                    show_edge=True,
                    box=box.ROUNDED,
                )

                for col in df.columns:
                    width = column_widths.get(col, 15)
                    col_name = str(col)
                    if len(col_name) > width:
                        col_name = col_name[: width - 3] + "..."

                    table.add_column(col_name, style="cyan", width=width, no_wrap=False)

                for _, row in df.iterrows():
                    formatted_row = []
                    for col in df.columns:
                        val = row[col]
                        str_val = str(val)

                        max_width = column_widths.get(col, 15)
                        if len(str_val) > max_width:
                            formatted_val = str_val[: max_width - 3] + "..."
                        else:
                            formatted_val = str_val
                        formatted_row.append(formatted_val)

                    table.add_row(*formatted_row)

                console.print(table)
        elif isinstance(df, pd.DataFrame) and df.empty:
            console.print("📭 没有返回数据", style="yellow")
        else:
            console.print(f"📊 结果: {df}", style="cyan")


@click.group(invoke_without_command=True)
@click.version_option(version="1.0.0", prog_name="QuackView")
@click.pass_context
def cli(ctx):
    """🦆 QuackView - Excel数据分析工具

    一个强大的Excel数据分析工具, 支持交互式分析和SQL查询。

    主要功能:
    • 交互式数据分析
    • 表结构查看
    • SQL查询执行
    • 多种数据格式输出

    支持的Excel格式: .xlsx, .xls
    """
    if ctx.invoked_subcommand is None:
        print_banner()
        print_usage_examples()
        console.print("\n💡 使用 'qv --help' 查看所有命令", style="yellow")
        console.print("💡 使用 'qv analyze --help' 查看分析命令详情", style="yellow")


@cli.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=str))
@click.option("--sheet-name", "-s", help="Excel工作表名(默认第一个工作表)")
@click.option("--head", "-h", default=5, help="显示前N行数据(默认5行)")
@click.option("--json", "output_json", is_flag=True, help="输出JSON格式(适合脚本处理)")
@click.option("--memory", is_flag=True, help="使用内存模式(默认启用)")
def analyze(
    file_path: str,
    sheet_name: Optional[str],
    head: int,
    output_json: bool,
    memory: bool,
):
    """🔍 交互式分析Excel文件

    启动交互式数据分析界面, 支持多种分析功能。

    示例:
        qv analyze sales.xlsx
        qv analyze data.xlsx --sheet-name "Sheet2" --head 10
        qv analyze report.xlsx --json

    交互式功能:
    • 数值统计 (SUM/AVG/MAX/MIN/COUNT)
    • 分组计数分析
    • 自定义过滤(开发中)
    • 表结构查看
    • 样本数据预览
    • 自定义SQL查询
    """
    try:
        service = create_memory_query_service()

        console.print(f"🔍 正在分析文件: {file_path}", style="bold")

        try:
            excel_file = pd.ExcelFile(file_path)
            console.print(f"📁 文件包含工作表: {excel_file.sheet_names}", style="cyan")
        except Exception:
            pass

        import_result = service.import_excel(
            file_path, sheet_name=sheet_name if sheet_name else 0
        )

        if not import_result["success"]:
            console.print(f"❌ 导入失败: {import_result['error']}", style="red")
            console.print("💡 请检查文件格式是否正确(支持.xlsx, .xls)", style="yellow")
            return

        table_name = import_result["table_name"]
        console.print(f"✅ 成功导入表: {table_name}", style="green")

        table_info = service.get_table_info()
        display_table_info(table_info)

        sample_data = service.get_sample_data(head)
        display_sample_data(sample_data, head)

        if output_json:
            result = {
                "table_name": table_name,
                "table_info": table_info,
                "sample_data": (
                    sample_data.to_dict(orient="records")
                    if not sample_data.empty
                    else []
                ),
            }
            console.print(
                json.dumps(result, ensure_ascii=False, indent=2, cls=PandasJSONEncoder)
            )
            return

        console.print("\n" + "=" * 60)
        console.print("🎯 交互式分析菜单", style="bold magenta")
        console.print("=" * 60)

        while True:
            console.print("\n📋 请选择分析类型:", style="bold")
            console.print("1. 📊 数值统计分析 (适用于数值型列)")
            console.print("2. 📝 文本数据分析 (适用于字符串型列)")
            console.print("3. 📅 时间序列分析 (适用于时间型列)")
            console.print("4. 🔍 数据质量检查")
            console.print("5. 📈 相关性分析")
            console.print("6. 📋 查看表结构")
            console.print("7. 👀 查看样本数据")
            console.print("8. 💻 执行自定义SQL")
            console.print("0. 🚪 退出")

            choice = Prompt.ask(
                "请选择",
                choices=["0", "1", "2", "3", "4", "5", "6", "7", "8"],
                default="1",
            )

            if choice == "0":
                break
            elif choice == "1":
                _handle_numeric_analysis(service)
            elif choice == "2":
                _handle_text_analysis(service)
            elif choice == "3":
                _handle_time_analysis(service)
            elif choice == "4":
                _handle_data_quality_check(service)
            elif choice == "5":
                _handle_correlation_analysis(service)
            elif choice == "6":
                console.print("\n📋 表结构信息", style="bold cyan")
                display_table_info(table_info)
            elif choice == "7":
                console.print("\n👀 样本数据预览", style="bold cyan")
                limit = IntPrompt.ask("显示前几行数据?", default=10)
                sample_data = service.get_sample_data(limit)
                display_sample_data(sample_data, limit)
            elif choice == "8":
                console.print("\n💻 自定义SQL查询", style="bold cyan")
                console.print(
                    "💡 支持标准SQL语法, 表名默认为导入的表名", style="yellow"
                )
                console.print("💡 示例: SELECT * FROM data LIMIT 10", style="yellow")
                sql = Prompt.ask("请输入SQL查询语句")
                if sql:
                    result = service.execute_custom_sql(sql)
                    display_analysis_result(result)

        service.close()
        console.print("\n👋 感谢使用QuackView!", style="bold green")
        console.print("💡 如有问题, 请查看文档或提交Issue", style="cyan")

    except Exception as e:
        console.print(f"❌ 发生错误: {e}", style="red")
        console.print("💡 请检查文件路径和格式是否正确", style="yellow")
        sys.exit(1)


def _handle_numeric_analysis(service):
    """处理数值统计分析"""
    console.print("\n📊 数值统计分析", style="bold cyan")
    console.print("💡 适用于数值型列，提供基础统计和高级分析", style="yellow")

    column_types = service.get_column_types()
    numeric_columns = []

    for col_name, col_type in column_types.items():
        if is_duckdb_numeric_type(col_type):
            numeric_columns.append(col_name)

    if not numeric_columns:
        console.print("❌ 没有找到数值型列", style="red")
        return

    console.print("\n📋 可用数值列:", style="bold")
    for i, col in enumerate(numeric_columns, 1):
        console.print(f"  {i}. {col} ({column_types[col]})", style="cyan")

    try:
        col_choice = IntPrompt.ask("请选择要分析的列", default=1)
        if 1 <= col_choice <= len(numeric_columns):
            selected_col = numeric_columns[col_choice - 1]

            console.print("\n📊 请选择分析操作:", style="bold")
            console.print("1. 📈 基础统计 (平均值、最大值、最小值、总和)")
            console.print("2. 📊 分布分析 (中位数、四分位数、百分位数)")
            console.print("3. 📉 变异分析 (方差、标准差、变异系数)")
            console.print("4. 🔍 异常值检测")
            console.print("5. 📋 缺失值分析")
            console.print("0. 🔙 返回主菜单")

            analysis_choice = Prompt.ask(
                "请选择", choices=["0", "1", "2", "3", "4", "5"], default="1"
            )

            if analysis_choice == "0":
                return
            elif analysis_choice == "1":
                result = service.execute_analysis(selected_col, "avg")
                display_analysis_result(result)
                result = service.execute_analysis(selected_col, "max")
                display_analysis_result(result)
                result = service.execute_analysis(selected_col, "min")
                display_analysis_result(result)
                result = service.execute_analysis(selected_col, "sum")
                display_analysis_result(result)
            elif analysis_choice == "2":
                result = service.execute_analysis(selected_col, "median")
                display_analysis_result(result)
                result = service.execute_analysis(selected_col, "quartiles")
                display_analysis_result(result)
                result = service.execute_analysis(selected_col, "percentiles")
                display_analysis_result(result)
            elif analysis_choice == "3":
                result = service.execute_analysis(selected_col, "var_pop")
                display_analysis_result(result)
                result = service.execute_analysis(selected_col, "stddev_pop")
                display_analysis_result(result)
            elif analysis_choice == "4":
                result = service.execute_analysis(selected_col, "quartiles")
                display_analysis_result(result)
            elif analysis_choice == "5":
                result = service.execute_analysis(selected_col, "missing_values")
                display_analysis_result(result)
        else:
            console.print("❌ 无效选择", style="red")
    except ValueError:
        console.print("❌ 请输入有效数字", style="red")


def _handle_text_analysis(service):
    """处理文本数据分析"""
    console.print("\n📝 文本数据分析", style="bold cyan")
    console.print("💡 适用于字符串型列，提供文本特征分析", style="yellow")

    column_types = service.get_column_types()
    text_columns = []

    for col_name, col_type in column_types.items():
        if is_duckdb_text_type(col_type):
            text_columns.append(col_name)

    if not text_columns:
        console.print("❌ 没有找到文本型列", style="red")
        return

    console.print("\n📋 可用文本列:", style="bold")
    for i, col in enumerate(text_columns, 1):
        console.print(f"  {i}. {col} ({column_types[col]})", style="cyan")

    try:
        col_choice = IntPrompt.ask("请选择要分析的列", default=1)
        if 1 <= col_choice <= len(text_columns):
            selected_col = text_columns[col_choice - 1]

            console.print("\n📝 请选择分析操作:", style="bold")
            console.print("1. 🔢 唯一值统计")
            console.print("2. 🏆 Top-K 分析")
            console.print("3. 📊 值分布分析")
            console.print("4. 📏 字符串长度分析")
            console.print("5. 🔍 模式识别")
            console.print("0. 🔙 返回主菜单")

            analysis_choice = Prompt.ask(
                "请选择", choices=["0", "1", "2", "3", "4", "5"], default="1"
            )

            if analysis_choice == "0":
                return
            elif analysis_choice == "1":
                result = service.execute_analysis(selected_col, "distinct_count")
                display_analysis_result(result)
            elif analysis_choice == "2":
                k_value = IntPrompt.ask("请输入K值", default=10)
                result = service.execute_analysis(selected_col, "top_k", top_k=k_value)
                display_analysis_result(result)
            elif analysis_choice == "3":
                result = service.execute_analysis(selected_col, "value_distribution")
                display_analysis_result(result)
            elif analysis_choice == "4":
                result = service.execute_analysis(selected_col, "length_analysis")
                display_analysis_result(result)
            elif analysis_choice == "5":
                result = service.execute_analysis(selected_col, "pattern_analysis")
                display_analysis_result(result)
        else:
            console.print("❌ 无效选择", style="red")
    except ValueError:
        console.print("❌ 请输入有效数字", style="red")


def _handle_time_analysis(service):
    """处理时间序列分析"""
    console.print("\n📅 时间序列分析", style="bold cyan")
    console.print("💡 适用于时间型列，提供时间维度分析", style="yellow")

    column_types = service.get_column_types()
    time_columns = []

    for col_name, col_type in column_types.items():
        if is_duckdb_time_type(col_type):
            time_columns.append(col_name)

    if not time_columns:
        console.print("❌ 没有找到时间型列", style="red")
        return

    console.print("\n📋 可用时间列:", style="bold")
    for i, col in enumerate(time_columns, 1):
        console.print(f"  {i}. {col} ({column_types[col]})", style="cyan")

    try:
        col_choice = IntPrompt.ask("请选择要分析的列", default=1)
        if 1 <= col_choice <= len(time_columns):
            selected_col = time_columns[col_choice - 1]

            console.print("\n📅 请选择分析操作:", style="bold")
            console.print("1. 📅 时间范围分析")
            console.print("2. 📊 年度分析")
            console.print("3. 📊 月度分析")
            console.print("4. 📊 日期分析")
            console.print("5. 📊 小时分析")
            console.print("6. 📊 星期分析")
            console.print("7. 🌸 季节性分析")
            console.print("0. 🔙 返回主菜单")

            analysis_choice = Prompt.ask(
                "请选择", choices=["0", "1", "2", "3", "4", "5", "6", "7"], default="1"
            )

            if analysis_choice == "0":
                return
            elif analysis_choice == "1":
                result = service.execute_analysis(selected_col, "date_range")
                display_analysis_result(result)
            elif analysis_choice == "2":
                result = service.execute_analysis(selected_col, "year_analysis")
                display_analysis_result(result)
            elif analysis_choice == "3":
                result = service.execute_analysis(selected_col, "month_analysis")
                display_analysis_result(result)
            elif analysis_choice == "4":
                result = service.execute_analysis(selected_col, "day_analysis")
                display_analysis_result(result)
            elif analysis_choice == "5":
                result = service.execute_analysis(selected_col, "hour_analysis")
                display_analysis_result(result)
            elif analysis_choice == "6":
                result = service.execute_analysis(selected_col, "weekday_analysis")
                display_analysis_result(result)
            elif analysis_choice == "7":
                result = service.execute_analysis(selected_col, "seasonal_analysis")
                display_analysis_result(result)
        else:
            console.print("❌ 无效选择", style="red")
    except ValueError:
        console.print("❌ 请输入有效数字", style="red")


def _handle_data_quality_check(service):
    """处理数据质量检查"""
    console.print("\n🔍 数据质量检查", style="bold cyan")
    console.print("💡 检查数据完整性、一致性和质量", style="yellow")

    column_types = service.get_column_types()

    console.print("\n📋 请选择要检查的列:", style="bold")
    for i, (col_name, col_type) in enumerate(column_types.items(), 1):
        console.print(f"  {i}. {col_name} ({col_type})", style="cyan")

    try:
        col_choice = IntPrompt.ask("请选择要检查的列", default=1)
        if 1 <= col_choice <= len(column_types):
            selected_col = list(column_types.keys())[col_choice - 1]

            console.print("\n🔍 请选择检查类型:", style="bold")
            console.print("1. 📊 缺失值分析")
            console.print("2. 📊 数据质量统计")
            console.print("0. 🔙 返回主菜单")

            analysis_choice = Prompt.ask("请选择", choices=["0", "1", "2"], default="1")

            if analysis_choice == "0":
                return
            elif analysis_choice == "1":
                result = service.execute_analysis(selected_col, "missing_values")
                display_analysis_result(result)
            elif analysis_choice == "2":
                result = service.execute_analysis(selected_col, "data_quality")
                display_analysis_result(result)
        else:
            console.print("❌ 无效选择", style="red")
    except ValueError:
        console.print("❌ 请输入有效数字", style="red")


def _handle_correlation_analysis(service):
    """处理相关性分析"""
    console.print("\n📈 相关性分析", style="bold cyan")
    console.print("💡 分析数值列之间的相关性", style="yellow")

    column_types = service.get_column_types()
    numeric_columns = []

    for col_name, col_type in column_types.items():
        if is_duckdb_numeric_type(col_type):
            numeric_columns.append(col_name)

    if len(numeric_columns) < 2:
        console.print("❌ 需要至少两个数值型列进行相关性分析", style="red")
        return

    console.print("\n📋 可用数值列:", style="bold")
    for i, col in enumerate(numeric_columns, 1):
        console.print(f"  {i}. {col} ({column_types[col]})", style="cyan")

    try:
        col1_choice = IntPrompt.ask("请选择第一个列", default=1)
        col2_choice = IntPrompt.ask("请选择第二个列", default=2)

        if (
            1 <= col1_choice <= len(numeric_columns)
            and 1 <= col2_choice <= len(numeric_columns)
            and col1_choice != col2_choice
        ):

            col1 = numeric_columns[col1_choice - 1]
            col2 = numeric_columns[col2_choice - 1]

            console.print(f"\n📈 分析 {col1} 和 {col2} 的相关性", style="bold")

            result = service.execute_analysis(col1, "correlation", second_column=col2)
            display_analysis_result(result)
        else:
            console.print("❌ 无效选择或选择了相同的列", style="red")
    except ValueError:
        console.print("❌ 请输入有效数字", style="red")


@cli.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=str))
@click.option("--sheet-name", "-s", help="Excel工作表名(默认第一个工作表)")
@click.option("--json", "output_json", is_flag=True, help="输出JSON格式(适合脚本处理)")
def schema(file_path: str, sheet_name: Optional[str], output_json: bool):
    """📋 查看Excel文件表结构

    显示Excel文件的表结构信息, 包括列名、数据类型、可空性等。

    示例:
        qv schema data.xlsx
        qv schema data.xlsx --sheet-name "Sheet2"
        qv schema report.xlsx --json

    输出信息:
    • 列名和数据类型
    • 是否允许空值
    • 默认值
    • 总行数
    """
    try:
        service = create_memory_query_service()

        console.print(f"📋 正在分析表结构: {file_path}", style="bold")

        import_result = service.import_excel(
            file_path, sheet_name=sheet_name if sheet_name else 0
        )

        if not import_result["success"]:
            console.print(f"❌ 导入失败: {import_result['error']}", style="red")
            console.print("💡 请检查文件格式是否正确(支持.xlsx, .xls)", style="yellow")
            return

        table_info = service.get_table_info()

        if output_json:
            console.print(
                json.dumps(
                    table_info, ensure_ascii=False, indent=2, cls=PandasJSONEncoder
                )
            )
        else:
            display_table_info(table_info)

        service.close()

    except Exception as e:
        console.print(f"❌ 发生错误: {e}", style="red")
        console.print("💡 请检查文件路径和格式是否正确", style="yellow")
        sys.exit(1)


@cli.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=str))
@click.argument("sql_query", type=str)
@click.option("--sheet-name", "-s", help="Excel工作表名(默认第一个工作表)")
@click.option("--json", "output_json", is_flag=True, help="输出JSON格式(适合脚本处理)")
def query(file_path: str, sql_query: str, sheet_name: Optional[str], output_json: bool):
    """💻 执行自定义SQL查询

    对Excel文件执行SQL查询并返回结果。

    示例:
        qv query data.xlsx "SELECT * FROM data LIMIT 10"
        qv query sales.xlsx "SELECT SUM(amount) FROM sales"
        qv query report.xlsx "SELECT COUNT(*) FROM report" --json

    支持的SQL功能:
    • SELECT 查询
    • WHERE 条件过滤
    • GROUP BY 分组
    • ORDER BY 排序
    • LIMIT 限制结果
    • 聚合函数 (SUM, AVG, COUNT, MAX, MIN)

    注意事项:
    • 表名默认为导入的表名
    • 列名区分大小写
    • 字符串值需要用单引号包围
    """
    try:
        service = create_memory_query_service()

        console.print(f"💻 正在执行SQL查询: {file_path}", style="bold")
        console.print(f"🔍 SQL语句: {sql_query}", style="cyan")

        import_result = service.import_excel(
            file_path, sheet_name=sheet_name if sheet_name else 0
        )

        if not import_result["success"]:
            console.print(f"❌ 导入失败: {import_result['error']}", style="red")
            console.print("💡 请检查文件格式是否正确(支持.xlsx, .xls)", style="yellow")
            return

        result = service.execute_custom_sql(sql_query)

        if output_json:
            result_data = result.get("result")
            if result_data is not None and isinstance(result_data, pd.DataFrame):
                serialized_result = []
                for _, row in result_data.iterrows():
                    row_dict = {}
                    for col, val in row.items():
                        if pd.isna(val):
                            row_dict[col] = None
                        elif isinstance(val, pd.Timestamp):
                            row_dict[col] = val.isoformat()
                        elif hasattr(val, "dtype"):
                            row_dict[col] = str(val)
                        else:
                            row_dict[col] = val
                    serialized_result.append(row_dict)
            else:
                serialized_result = result_data

            json_result = {
                "success": result.get("success"),
                "sql": result.get("sql"),
                "error": result.get("error"),
                "result": serialized_result,
            }
            console.print(json.dumps(json_result, ensure_ascii=False, indent=2))
        else:
            display_analysis_result(result)

        service.close()

    except Exception as e:
        console.print(f"❌ 发生错误: {e}", style="red")
        console.print("💡 请检查SQL语法是否正确", style="yellow")
        console.print("💡 示例: SELECT * FROM data LIMIT 10", style="yellow")
        sys.exit(1)


@cli.command()
def version():
    """📋 显示版本信息

    显示QuackView的版本信息和基本介绍。
    """
    console.print("🦆 QuackView v1.0.0", style="bold blue")
    console.print("📊 Excel数据分析工具", style="cyan")


if __name__ == "__main__":
    cli()
