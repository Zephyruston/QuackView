# QuackView API - Success Scenarios Tests
# Tests for successful API operations

# Test 1: Health check
GET http://localhost:8000/health
HTTP 200
[Asserts]
jsonpath "$.status" == "healthy"
jsonpath "$.service" == "QuackView API"

# Test 2: Upload valid Excel file and create session
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.task_id" != null
jsonpath "$.task_id" != ""

# Capture the task_id for subsequent tests
[Captures]
task_id: jsonpath "$.task_id"

# Test 3: Get schema information
GET http://localhost:8000/api/schema?task_id={{task_id}}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.table_name" != null
jsonpath "$.columns" != null
jsonpath "$.columns" count >= 0

# Test 4: Get analysis options
GET http://localhost:8000/api/analysis-options?task_id={{task_id}}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.options" count >= 0

# Test 5: Execute analysis with SUM operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "SUM"
    }
  ]
}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.columns" count >= 0
jsonpath "$.rows" count >= 0
jsonpath "$.sql_preview" contains "SELECT"

# Test 6: Execute analysis with multiple operations
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "SUM"
    },
    {
      "column": "Price",
      "operation": "AVG"
    },
    {
      "column": "Price",
      "operation": "COUNT"
    }
  ]
}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.columns" count >= 0
jsonpath "$.rows" count >= 0
jsonpath "$.sql_preview" contains "SUM"
jsonpath "$.sql_preview" contains "AVG"
jsonpath "$.sql_preview" contains "COUNT"

# Test 7: Execute analysis with filters
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "SUM"
    }
  ],
  "filters": [
    {
      "column": "Id",
      "operator": "=",
      "value": 1
    }
  ]
}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.columns" count >= 0
jsonpath "$.rows" count >= 0
jsonpath "$.sql_preview" contains "WHERE"

# Test 8: Execute custom SQL query
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "sql": "SELECT * FROM data LIMIT 5"
}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.columns" count >= 0
jsonpath "$.rows" count >= 0
jsonpath "$.sql_preview" contains "SELECT"

# Test 9: Get session info
GET http://localhost:8000/api/session-info?task_id={{task_id}}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.task_id" == "{{task_id}}"
jsonpath "$.table_name" != null
jsonpath "$.file_path" != null
jsonpath "$.created_at" != null

# Test 10: Export SQL
GET http://localhost:8000/api/export/sql?task_id={{task_id}}
HTTP 200
[Asserts]
header "content-type" contains "text/sql"
header "content-disposition" contains "attachment"

# Test 11: Export Excel
GET http://localhost:8000/api/export/excel?task_id={{task_id}}
HTTP 200
[Asserts]
header "content-type" contains "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
header "content-disposition" contains "attachment"

# Test 12: Close session
DELETE http://localhost:8000/api/connection?task_id={{task_id}}
HTTP 200
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.message" == "Session closed successfully"

# Test 13: Verify session is closed by trying to access it
GET http://localhost:8000/api/schema?task_id={{task_id}}
HTTP 404
[Asserts]
header "content-type" contains "application/json"
jsonpath "$.error" == "SESSION_NOT_FOUND" 