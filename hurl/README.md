# QuackView API 测试

此目录包含用于 QuackView RESTful API 的 Hurl 测试文件。

## 测试结构

- `01_connection.hurl` - 文件上传和会话管理测试
- `02_schema.hurl` - 模式和元数据检索测试
- `03_analysis.hurl` - 数据分析操作测试
- `04_export.hurl` - 导出功能测试
- `05_custom_query.hurl` - 自定义 SQL 查询测试
- `06_error_handling.hurl` - 错误处理和边界情况测试
- `07_success_scenarios.hurl` - 成功场景完整流程测试

## 错误处理机制

### 自定义异常类型

- `SESSION_NOT_FOUND` - 会话不存在 (404)
- `FILE_VALIDATION_ERROR` - 文件验证错误 (400)
- `EXCEL_PROCESSING_ERROR` - Excel 处理错误 (422)
- `SQL_EXECUTION_ERROR` - SQL 执行错误 (422)
- `ANALYSIS_ERROR` - 分析任务错误 (422)
- `EXPORT_ERROR` - 导出错误 (500)
- `DATABASE_CONNECTION_ERROR` - 数据库连接错误 (500)
- `INVALID_REQUEST_ERROR` - 无效请求错误 (400)

### 统一错误响应格式

```json
{
  "error": "ERROR_CODE",
  "detail": "错误描述信息"
}
```

## 运行测试

```bash
# 运行所有测试
hurl --test *.hurl
```

## 测试数据

测试使用当前目录中的样本 Excel 文件 `data.xlsx`。
