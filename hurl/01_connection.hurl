# QuackView API - Connection Management Tests
# Tests for file upload and session management

# Test 1: Health check
GET http://localhost:8000/health
HTTP 200
[Asserts]
jsonpath "$.status" == "healthy"
jsonpath "$.service" == "QuackView API"

# Test 2: Upload Excel file and create session
POST http://localhost:8000/api/connection
[Multipart]
file: file,./data.xlsx;
HTTP 200
[Captures]
task_id: jsonpath "$.task_id"
[Asserts]
jsonpath "$.task_id" exists
jsonpath "$.task_id" matches /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

# Test 3: Retrieve session info
GET http://localhost:8000/api/session-info?task_id={{task_id}}
HTTP 200
[Asserts]
jsonpath "$.task_id" == {{task_id}}
jsonpath "$.table_name" exists
jsonpath "$.file_path" exists
jsonpath "$.created_at" exists

# Test 4: Attempt to upload non-Excel file (should fail)
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
jsonpath "$.detail" == "Only Excel files are supported"

# Test 5: Close session
DELETE http://localhost:8000/api/connection?task_id={{task_id}}
HTTP 200
[Asserts]
jsonpath "$.message" == "Session closed successfully"

# Test 6: Attempt to close non-existent session (should fail)
DELETE http://localhost:8000/api/connection?task_id=non-existent-task-id
HTTP 404
[Asserts]
jsonpath "$.detail" == "Session not found" 