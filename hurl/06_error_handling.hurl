# QuackView API - Error Handling Tests
# Tests for various error scenarios and edge cases
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Captures]
task_id: jsonpath "$.task_id"

# Test 1: Health check (should always work)
GET http://localhost:8000/health
HTTP 200

# Test 2: Upload unsupported file type
POST http://localhost:8000/api/connection
Content-Type: multipart/form-data; boundary=boundary
```
--boundary
Content-Disposition: form-data; name="file"; filename="test.txt"
Content-Type: text/plain

Hello World!
--boundary--
```
HTTP 400
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "FILE_VALIDATION_ERROR"
jsonpath "$.detail" contains "Only Excel files"

# Test 3: Upload empty file
POST http://localhost:8000/api/connection
Content-Type: multipart/form-data; boundary=boundary
```
--boundary
Content-Disposition: form-data; name="file"; filename="empty.xlsx"
Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet

--boundary--
```
HTTP 422

# Test 4: Upload without file parameter
POST http://localhost:8000/api/connection
Content-Type: multipart/form-data; boundary=boundary
```
--boundary
Content-Disposition: form-data; name="other"; filename="test.txt"

Hello World!
--boundary--
```
HTTP 422

# Test 5: Try to get schema without taskId parameter
GET http://localhost:8000/api/schema
HTTP 422

# Test 6: Try to get analysis options without taskId parameter
GET http://localhost:8000/api/analysis-options
HTTP 422

# Test 7: Try to export SQL without taskId parameter
GET http://localhost:8000/api/export/sql
HTTP 422

# Test 8: Try to export Excel without taskId parameter
GET http://localhost:8000/api/export/excel
HTTP 422

# Test 9: Try to close connection without taskId parameter
DELETE http://localhost:8000/api/connection
HTTP 422

# Test 10: Try to get schema with invalid taskId
GET http://localhost:8000/api/schema?task_id=invalid_session_id
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND"
jsonpath "$.detail" contains "Session invalid_session_id not found"

# Test 11: Try to get analysis options with invalid taskId
GET http://localhost:8000/api/analysis-options?task_id=invalid_session_id
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND"

# Test 12: Try to export SQL with invalid taskId
GET http://localhost:8000/api/export/sql?task_id=invalid_session_id
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND"

# Test 13: Try to export Excel with invalid taskId
GET http://localhost:8000/api/export/excel?task_id=invalid_session_id
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND"

# Test 14: Try to close connection with invalid taskId
DELETE http://localhost:8000/api/connection?task_id=invalid_session_id
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND"

# Test 15: Analysis request with invalid taskId
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "invalid_session_id",
  "operations": [
    {
      "column": "sales",
      "operation": "SUM"
    }
  ]
}
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND"

# Test 16: Analysis request with no operations
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": []
}
HTTP 422
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "ANALYSIS_ERROR"
jsonpath "$.detail" contains "No analysis operations provided"

# Test 17: Analysis request with unsupported operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "sales",
      "operation": "INVALID_OP"
    }
  ]
}
HTTP 422
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "ANALYSIS_ERROR"
jsonpath "$.detail" contains "Unsupported operation"

# Test 18: Custom query with invalid taskId
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "invalid_session_id",
  "sql": "SELECT * FROM data"
}
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND"

# Test 19: Custom query with empty SQL
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "sql": ""
}
HTTP 422
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SQL_EXECUTION_ERROR"
jsonpath "$.detail" contains "SQL query is empty"

# Test 20: Custom query with invalid SQL
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "sql": "SELECT * FROM nonexistent_table"
}
HTTP 422
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SQL_EXECUTION_ERROR"

# Test 21: Missing required fields in analysis request
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "test"
}
HTTP 422

# Test 22: Missing required fields in custom query request
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "test"
}
HTTP 422

# Test 23: Invalid JSON in analysis request
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "test",
  "operations": [
    {
      "column": "sales",
      "operation": "SUM"
    }
  ],
  "invalid_field": "should_fail"
}
HTTP 404

# Test 24: Invalid JSON in custom query request
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "test",
  "sql": "SELECT * FROM data",
  "extra_field": "should_fail"
}
HTTP 404