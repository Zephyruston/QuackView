# 🦆 QuackView - Excel 数据分析工具

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

## 🚀 快速开始

### 安装

```bash
uv venv
uv sync
```

### 基本使用

```bash
# 交互式分析Excel文件
qv analyze data.xlsx

# 查看表结构
qv schema data.xlsx

# 执行SQL查询
qv query data.xlsx "SELECT * FROM data LIMIT 10"
```

## 📖 详细使用指南

### 1. 交互式分析 (`analyze`)

启动交互式数据分析界面，支持多种分析功能。

```bash
# 基本用法
qv analyze sales.xlsx

# 指定工作表
qv analyze data.xlsx --sheet-name "Sheet2"

# 显示更多样本数据
qv analyze data.xlsx --head 20

# 输出JSON格式（适合脚本处理）
qv analyze report.xlsx --json
```

**交互式功能菜单：**

1. **📊 数值统计** - SUM/AVG/MAX/MIN/COUNT
2. **📈 分组计数分析** - 按列分组统计
3. **🔍 自定义过滤** - 条件过滤（开发中）
4. **📋 查看表结构** - 列信息详情
5. **👀 查看样本数据** - 数据预览
6. **💻 执行自定义 SQL** - 自由查询

### 文件格式支持

- ✅ `.xlsx` - Excel 2007+ 格式
- ✅ `.xls` - Excel 97-2003 格式

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件。
