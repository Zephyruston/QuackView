# QuackView API - Analysis Tests
# Tests for data analysis functionality

# Setup: Upload file and get task_id
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Captures]
task_id: jsonpath "$.task_id"

# Test 1: Simple analysis with SUM operation
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
jsonpath "$.columns" exists
jsonpath "$.rows" exists
jsonpath "$.sql_preview" exists
jsonpath "$.sql_preview" contains "SUM"

# Test 2: Analysis with multiple operations
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
      "column": "Product",
      "operation": "COUNT_DISTINCT"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.columns" count == 2
jsonpath "$.sql_preview" contains "SUM"
jsonpath "$.sql_preview" contains "COUNT(DISTINCT"

# Test 3: Analysis with filters
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "AVG"
    }
  ],
  "filters": [
    {
      "column": "Product",
      "operator": "=",
      "value": "Laptop"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "WHERE"
jsonpath "$.sql_preview" contains "AVG"

# Test 4: Analysis with BETWEEN filter
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "MAX"
    }
  ],
  "filters": [
    {
      "column": "Date",
      "operator": "BETWEEN",
      "value": ["2023-01-01", "2023-12-31"]
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "BETWEEN"

# Test 5: Try analysis with non-existent session
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "non-existent-task-id",
  "operations": [
    {
      "column": "Price",
      "operation": "SUM"
    }
  ]
}
HTTP 404
[Asserts]
jsonpath "$.detail" contains "not found"

# Cleanup: Close session
DELETE http://localhost:8000/api/connection?task_id={{task_id}}
HTTP 200 