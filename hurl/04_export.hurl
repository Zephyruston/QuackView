# QuackView API - Export Tests
# Tests for SQL and Excel export functionality

# Setup: Upload file and get task_id
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Captures]
task_id: jsonpath "$.task_id"

# Test 1: Export SQL script
GET http://localhost:8000/api/export/sql?task_id={{task_id}}
HTTP 200
[Asserts]
header "Content-Type" contains "text/sql"
header "Content-Disposition" contains "quackview_export_"
header "Content-Disposition" contains ".sql"

# Test 2: Export Excel file
GET http://localhost:8000/api/export/excel?task_id={{task_id}}
HTTP 200
[Asserts]
header "Content-Type" == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
header "Content-Disposition" contains "quackview_export_"
header "Content-Disposition" contains ".xlsx"

# Test 3: Try to export SQL for non-existent session
GET http://localhost:8000/api/export/sql?task_id=non-existent-task-id
HTTP 404
[Asserts]
jsonpath "$.detail" contains "not found"

# Test 4: Try to export Excel for non-existent session
GET http://localhost:8000/api/export/excel?task_id=non-existent-task-id
HTTP 404
[Asserts]
jsonpath "$.detail" contains "not found"

# Test 5: Export analysis result as Excel
POST http://localhost:8000/api/export/result-excel
Content-Type: application/json
{
  "columns": ["A", "B"],
  "rows": [[1, 2], [3, 4]]
}
HTTP 200
[Asserts]
header "Content-Type" == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
header "Content-Disposition" contains "quackview_result_export.xlsx"

# Cleanup: Close session
DELETE http://localhost:8000/api/connection?task_id={{task_id}}
HTTP 200 