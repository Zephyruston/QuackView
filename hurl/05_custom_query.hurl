# QuackView API - Custom Query Tests
# Tests for custom SQL query functionality

# Setup: Upload file and get task_id
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Captures]
task_id: jsonpath "$.task_id"

# Get session info to get table name
GET http://localhost:8000/api/session-info?task_id={{task_id}}
HTTP 200
[Captures]
table_name: jsonpath "$.table_name"

# Test 1: Simple SELECT query
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "sql": "SELECT * FROM {{table_name}} LIMIT 5"
}
HTTP 200
[Asserts]
jsonpath "$.columns" exists
jsonpath "$.rows" exists
jsonpath "$.sql_preview" contains "SELECT * FROM"

# Test 2: Aggregation query
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "sql": "SELECT COUNT(*) as total_rows FROM {{table_name}}"
}
HTTP 200
[Asserts]
jsonpath "$.columns" count == 1
jsonpath "$.columns[0]" == "total_rows"

# Test 3: Complex query with WHERE clause
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "sql": "SELECT Product, SUM(Price) as total_price FROM {{table_name}} WHERE Product = 'Laptop' GROUP BY Product"
}
HTTP 200
[Asserts]
jsonpath "$.columns" count == 2
jsonpath "$.sql_preview" contains "WHERE"

# Test 4: Invalid SQL query (should fail)
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "sql": "SELECT * FROM non_existent_table"
}
HTTP 404
[Asserts]
jsonpath "$.detail" contains "SQL执行错误"

# Test 5: Try custom query with non-existent session
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "non-existent-task-id",
  "sql": "SELECT * FROM data"
}
HTTP 404
[Asserts]
jsonpath "$.detail" contains "not found"

# Cleanup: Close session
DELETE http://localhost:8000/api/connection?task_id={{task_id}}
HTTP 200 