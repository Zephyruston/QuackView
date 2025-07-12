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
# 克隆项目
git clone https://github.com/Zephyruston/QuackView.git
cd QuackView

# 创建虚拟环境并安装依赖
uv venv
uv sync
```

### 基本使用

```bash
# 启动后端服务 port 8000
uv run -m app.api.main

# 交互式分析Excel文件
qv analyze data.xlsx

# 查看表结构
qv schema data.xlsx

# 执行SQL查询
qv query data.xlsx "SELECT * FROM data LIMIT 10"

# 查看版本信息
qv version
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

#### 📊 数值统计分析

适用于数值型列，提供基础统计和高级分析：

- **基础统计** - 平均值、最大值、最小值、总和
- **分布分析** - 中位数、四分位数、百分位数
- **变异分析** - 方差、标准差、变异系数
- **异常值检测** - 基于四分位数的异常值识别
- **缺失值分析** - 数据完整性检查

#### 📝 文本数据分析

适用于字符串型列，提供文本特征分析：

- **唯一值统计** - 统计不同值的数量
- **Top-K 分析** - 获取前 K 个最常见值
- **值分布分析** - 分析值的分布情况
- **字符串长度分析** - 分析字符串长度分布
- **模式识别** - 识别文本模式

#### 📅 时间序列分析

适用于时间型列，提供时间维度分析：

- **时间范围分析** - 计算最早和最晚时间
- **年度分析** - 按年度聚合统计
- **月度分析** - 按月度聚合统计
- **日期分析** - 按日期聚合统计
- **小时分析** - 按小时聚合统计
- **星期分析** - 按星期聚合统计
- **季节性分析** - 按季节聚合统计

#### 🔍 数据质量检查

检查数据完整性、一致性和质量：

- **缺失值分析** - 统计缺失值情况
- **数据质量统计** - 数据完整性检查

#### 📈 相关性分析

分析数值列之间的相关性：

- **皮尔逊相关系数** - 计算两个数值列的相关性

#### 📋 其他功能

- **查看表结构** - 列信息详情
- **查看样本数据** - 数据预览
- **执行自定义 SQL** - 自由查询

### 2. 表结构查看 (`schema`)

显示 Excel 文件的表结构信息，包括列名、数据类型、可空性等。

```bash
# 基本用法
qv schema data.xlsx

# 指定工作表
qv schema data.xlsx --sheet-name "Sheet2"

# 输出JSON格式
qv schema report.xlsx --json
```

**输出信息：**

- 列名和数据类型
- 是否允许空值
- 默认值
- 总行数

### 3. SQL 查询执行 (`query`)

对 Excel 文件执行 SQL 查询并返回结果。

```bash
# 基本查询
qv query data.xlsx "SELECT * FROM data LIMIT 10"

# 聚合查询
qv query sales.xlsx "SELECT SUM(amount) FROM sales"

# 统计查询
qv query report.xlsx "SELECT COUNT(*) FROM report" --json
```

**支持的 SQL 功能：**

- SELECT 查询
- WHERE 条件过滤
- GROUP BY 分组
- ORDER BY 排序
- LIMIT 限制结果
- 聚合函数 (SUM, AVG, COUNT, MAX, MIN)

**注意事项：**

- 表名默认为导入的表名
- 列名区分大小写
- 字符串值需要用单引号包围

### 文件格式支持

- ✅ `.xlsx` - Excel 2007+ 格式
- ✅ `.xls` - Excel 97-2003 格式

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件。
