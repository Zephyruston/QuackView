# QuackView API - SQL Generation Tests
# Tests for enhanced SQL generation functionality

# Setup: Upload file and get task_id
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Captures]
task_id: jsonpath "$.task_id"

# Test 1: Get analysis options with enhanced types
GET http://localhost:8000/api/analysis-options?task_id={{task_id}}
HTTP 200
[Asserts]
jsonpath "$.options" exists
jsonpath "$.options" count > 0
jsonpath "$.options[0].column" exists
jsonpath "$.options[0].operations" exists
jsonpath "$.options[0].operations" count > 0

# Test 2: Test VAR_POP operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "VAR_POP"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.columns" exists
jsonpath "$.rows" exists
jsonpath "$.sql_preview" contains "VAR_POP"

# Test 3: Test STDDEV_POP operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "STDDEV_POP"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "STDDEV_POP"

# Test 4: Test MEDIAN operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "MEDIAN"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "PERCENTILE_CONT"

# Test 5: Test QUARTILES operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "QUARTILES"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "PERCENTILE_CONT"

# Test 6: Test PERCENTILES operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "PERCENTILES"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "PERCENTILE_CONT"

# Test 7: Test TOP_K operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Product",
      "operation": "TOP_K"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "GROUP BY"
jsonpath "$.sql_preview" contains "ORDER BY"

# Test 8: Test VALUE_DISTRIBUTION operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Product",
      "operation": "VALUE_DISTRIBUTION"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "GROUP BY"

# Test 9: Test LENGTH_ANALYSIS operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Product",
      "operation": "LENGTH_ANALYSIS"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "LENGTH"

# Test 10: Test DATE_RANGE operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Date",
      "operation": "DATE_RANGE"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "MIN"
jsonpath "$.sql_preview" contains "MAX"

# Test 11: Test YEAR_ANALYSIS operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Date",
      "operation": "YEAR_ANALYSIS"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "EXTRACT(YEAR"

# Test 12: Test MONTH_ANALYSIS operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Date",
      "operation": "MONTH_ANALYSIS"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "EXTRACT(MONTH"

# Test 13: Test MISSING_VALUES operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "MISSING_VALUES"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "COUNT(*)"
jsonpath "$.sql_preview" contains "null_count"

# Test 14: Test DATA_QUALITY operation
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "DATA_QUALITY"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "COUNT(*)"
jsonpath "$.sql_preview" contains "distinct_count"

# Test 15: Test multiple operations with group by
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "AVG"
    },
    {
      "column": "Price",
      "operation": "SUM"
    }
  ],
  "group_by": ["Product"]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "AVG"
jsonpath "$.sql_preview" contains "SUM"
jsonpath "$.sql_preview" contains "GROUP BY"

# Test 16: Test with enhanced WHERE conditions
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
      "column": "Price",
      "operator": ">=",
      "value": 50
    },
    {
      "column": "Product",
      "operator": "LIKE",
      "value": "%Laptop%"
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "WHERE"
jsonpath "$.sql_preview" contains ">="
jsonpath "$.sql_preview" contains "LIKE"

# Test 17: Test BETWEEN condition
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "COUNT"
    }
  ],
  "filters": [
    {
      "column": "Price",
      "operator": "BETWEEN",
      "value": [50, 200]
    }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.sql_preview" contains "BETWEEN"

# Test 18: Test unsupported operation (should fail gracefully)
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "{{task_id}}",
  "operations": [
    {
      "column": "Price",
      "operation": "UNSUPPORTED_OPERATION"
    }
  ]
}
HTTP 422
[Asserts]
jsonpath "$.detail" contains "Unsupported operation"

# Cleanup: Close session
DELETE http://localhost:8000/api/connection?task_id={{task_id}}
HTTP 200 