# QuackView API - Error Handling Tests
# Tests for various error scenarios and edge cases

# Test 1: Health check (should always work)
GET http://localhost:8000/health
HTTP 200

# Test 2: Upload empty file
POST http://localhost:8000/api/connection
Content-Type: multipart/form-data; boundary=boundary
```
--boundary
Content-Disposition: form-data; name="file"; filename="empty.xlsx"
Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet

--boundary--
```
HTTP 422

# Test 3: Upload without file parameter
POST http://localhost:8000/api/connection
Content-Type: multipart/form-data; boundary=boundary
```
--boundary
Content-Disposition: form-data; name="other"; filename="test.txt"

Hello World!
--boundary--
```
HTTP 422

# Test 4: Try to get schema without taskId parameter
GET http://localhost:8000/api/schema
HTTP 422

# Test 5: Try to get analysis options without taskId parameter
GET http://localhost:8000/api/analysis-options
HTTP 422

# Test 6: Try to export SQL without taskId parameter
GET http://localhost:8000/api/export/sql
HTTP 422

# Test 7: Try to export Excel without taskId parameter
GET http://localhost:8000/api/export/excel
HTTP 422

# Test 8: Try to close connection without taskId parameter
DELETE http://localhost:8000/api/connection
HTTP 422

# Test 9: Invalid JSON in analysis request
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

# Test 10: Invalid JSON in custom query request
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "test",
  "sql": "SELECT * FROM data",
  "extra_field": "should_fail"
}
HTTP 404

# Test 11: Missing required fields in analysis request
POST http://localhost:8000/api/analyze
Content-Type: application/json
{
  "task_id": "test"
}
HTTP 422

# Test 12: Missing required fields in custom query request
POST http://localhost:8000/api/query/custom
Content-Type: application/json
{
  "task_id": "test"
}
HTTP 422 