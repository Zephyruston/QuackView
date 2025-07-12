# QuackView API - Schema and Metadata Tests
# Tests for schema retrieval and analysis options

# Setup: Upload file and get task_id
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Captures]
task_id: jsonpath "$.task_id"

# Test 1: Get schema information
GET http://localhost:8000/api/schema?task_id={{task_id}}
HTTP 200
[Asserts]
jsonpath "$.table_name" exists
jsonpath "$.columns" exists
jsonpath "$.columns" count > 0
jsonpath "$.columns[0].name" exists
jsonpath "$.columns[0].type" exists

# Test 2: Get analysis options
GET http://localhost:8000/api/analysis-options?task_id={{task_id}}
HTTP 200
[Asserts]
jsonpath "$.options" exists
jsonpath "$.options" count > 0
jsonpath "$.options[0].column" exists
jsonpath "$.options[0].operations" exists
jsonpath "$.options[0].operations" count > 0

# Test 3: Try to get schema for non-existent session
GET http://localhost:8000/api/schema?task_id=non-existent-task-id
HTTP 404
[Asserts]
jsonpath "$.detail" contains "not found"

# Test 4: Try to get analysis options for non-existent session
GET http://localhost:8000/api/analysis-options?task_id=non-existent-task-id
HTTP 404
[Asserts]
jsonpath "$.detail" contains "not found"

# Cleanup: Close session
DELETE http://localhost:8000/api/connection?task_id={{task_id}}
HTTP 200 