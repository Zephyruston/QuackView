# QuackView 项目测试文档

## 测试概述

本项目包含完整的单元测试套件，确保 `QuackView` 项目中各个模块的核心功能正确无误，包括数据导入、SQL 生成、SQL 执行、数据类型判断等。

## 测试目录结构

```
QuackView/
├── tests/
│   ├── __init__.py
│   ├── test_utils.py              # 工具函数测试
│   ├── test_sql_generator.py      # SQL生成器测试
│   ├── test_sql_executor.py       # SQL执行器测试
│   ├── test_integration.py        # 集成测试
│   ├── test_edge_cases.py         # 边界情况测试
│   ├── run_tests.py               # 测试运行脚本
│   └── README.md                  # 测试文档
```

## 测试内容

### 1. 工具函数测试 (`test_utils.py`)

测试 `app/utils/utils.py` 模块的功能：

- **数据类型判断**: 测试数值、文本、时间类型的判断函数
- **列类型映射**: 测试获取表结构的功能
- **JSON 编码器**: 测试 Pandas 数据类型的 JSON 序列化
- **边界情况**: 测试空表、复杂类型等边界情况

### 2. SQL 生成器测试 (`test_sql_generator.py`)

测试 `app/generator/sql_generator.py` 模块的功能：

- **分析类型获取**: 测试根据列类型获取可用分析类型
- **SQL 生成**: 测试各种分析类型的 SQL 生成
- **条件处理**: 测试 WHERE 条件、GROUP BY、LIMIT 等子句
- **多列分析**: 测试多列同时分析的 SQL 生成
- **模板完整性**: 确保所有分析类型都有对应的 SQL 模板

### 3. SQL 执行器测试 (`test_sql_executor.py`)

测试 `app/executor/sql_executor.py` 模块的功能：

- **SQL 执行**: 测试基本 SQL 查询的执行
- **执行计划**: 测试获取 SQL 执行计划
- **分析执行**: 测试各种分析类型的执行
- **错误处理**: 测试 SQL 执行错误的处理
- **表结构获取**: 测试获取表结构和样本数据

### 4. 集成测试 (`test_integration.py`)

测试整个工作流程的集成功能：

- **完整工作流**: 测试从数据获取到结果输出的完整流程
- **多类型分析**: 测试数值、文本、时间分析的完整流程
- **条件分析**: 测试带 WHERE 条件的分析流程
- **分组分析**: 测试带 GROUP BY 的分析流程
- **多列分析**: 测试多列同时分析的完整流程
- **错误处理**: 测试各种错误情况的处理

### 5. 边界情况测试 (`test_edge_cases.py`)

测试各种边界情况和异常情况：

- **NULL 值处理**: 测试 NULL 值的正确处理
- **空字符串**: 测试空字符串的处理
- **极值处理**: 测试极大值、极小值的处理
- **特殊字符**: 测试特殊字符和 Unicode 字符的处理
- **数据类型**: 测试各种数据类型的处理
- **大数据量**: 测试大数据量下的性能

## 运行测试

### 运行所有测试

```bash
# 在项目根目录下运行
uv run tests/run_tests.py
```

### 运行特定测试

```bash
# 运行特定测试类
uv run tests/test_edge_cases.py TestEdgeCases.test_null_values_handling

# 运行特定测试模块
uv run -m unittest tests.test_utils
```

### 运行覆盖率测试

```bash
# 安装coverage模块
uv pip install coverage

# 运行覆盖率测试
uv run tests/run_tests.py --coverage
```

## 测试数据

测试使用内存数据库（DuckDB），包含以下测试数据：

### 基础测试表

- `test_table`: 包含各种数据类型的测试数据
- `sales_data`: 模拟销售数据的完整测试表

### 边界情况测试表

- `edge_case_table`: 包含 NULL 值、空字符串、极值等边界情况
- `empty_table`: 空表测试
- `single_row_table`: 单行表测试
- `duplicate_table`: 重复值测试
- `special_chars_table`: 特殊字符测试
- `unicode_table`: Unicode 字符测试

## 注意事项

1. **内存数据库**: 所有测试使用内存数据库，不会影响实际数据
2. **测试隔离**: 每个测试用例都有独立的数据库环境
3. **数据清理**: 测试完成后自动清理测试数据
4. **错误处理**: 测试包含完整的错误处理验证
5. **性能测试**: 包含大数据量下的性能测试

## 扩展测试

如需添加新的测试，请遵循以下规范：

1. **测试文件命名**: `test_模块名.py`
2. **测试类命名**: `Test模块名`
3. **测试方法命名**: `test_功能描述`
4. **测试数据**: 使用内存数据库和测试数据
5. **测试隔离**: 每个测试独立运行，不依赖其他测试
6. **文档注释**: 为每个测试方法添加清晰的文档注释
