# QuackView API 测试

此目录包含用于 QuackView RESTful API 的 Hurl 测试文件。

## 测试结构

- `01_connection.hurl` - 文件上传和会话管理测试
- `02_schema.hurl` - 模式和元数据检索测试
- `03_analysis.hurl` - 数据分析操作测试
- `04_export.hurl` - 导出功能测试
- `05_custom_query.hurl` - 自定义 SQL 查询测试
- `06_error_handling.hurl` - 错误处理和边界情况测试

## 运行测试

```bash
# 运行所有测试
hurl --test *.hurl
```

## 测试数据

测试使用当前目录中的样本 Excel 文件 `data.xlsx`。
