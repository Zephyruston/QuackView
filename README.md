# 🦆 QuackView

> Click & Analyze.  
> 🧾 一键导入 Excel 表格，🧠 自动分析数据，用 🦆 DuckDB 做你的数据分析引擎。

## 🎯 项目简介

**QuackView** 是一款面向非程序员 / Excel 初学者设计的轻量数据分析工具。  
只需上传 Excel 表格，系统就会自动识别字段类型并提供可视化分析选项，如：平均数、分组统计、Top-N 等操作。  
底层由超快的 **DuckDB** 引擎驱动。

## 💡 灵感来源

- 非技术用户面对复杂函数容易畏难，分析数据不应成为负担。
- DuckDB 非常适合做本地分析型工具，提供现代 SQL 能力。
- 类似 `goctl` 的自动生成器思维：输入结构，输出能力。

## 运行示例

```bash
uv run -m examples.example_usage
```
